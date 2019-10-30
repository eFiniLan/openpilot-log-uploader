#!/usr/bin/env python3
import os
import time
import json
import random
import ctypes
import inspect
import requests
import traceback
import threading

from config import ROOT, dongle_id, delete_after_upload, fake_upload, dcamera_upload

from api import Api

def raise_on_thread(t, exctype):
  for ctid, tobj in threading._active.items():
    if tobj is t:
      tid = ctid
      break
  else:
    raise Exception("Could not find thread")

  '''Raises an exception in the threads with id tid'''
  if not inspect.isclass(exctype):
    raise TypeError("Only types can be raised (not instances)")

  res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid),
                                                   ctypes.py_object(exctype))
  if res == 0:
    raise ValueError("invalid thread id")
  elif res != 1:
    # "if it returns a number greater than one, you're in trouble,
    # and you should call it again with exc=NULL to revert the effect"
    ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
    raise SystemError("PyThreadState_SetAsyncExc failed")

def get_directory_sort(d):
  return list(map(lambda s: s.rjust(10, '0'), d.rsplit('--', 1)))

def listdir_by_creation(d):
  try:
    paths = os.listdir(d)
    paths = sorted(paths, key=get_directory_sort)
    return paths
  except OSError:
    print("listdir_by_creation failed")
    return list()

def clear_locks(root):
  for logname in os.listdir(root):
    path = os.path.join(root, logname)
    try:
      for fname in os.listdir(path):
        if fname.endswith(".lock"):
          os.unlink(os.path.join(path, fname))
    except OSError:
      print("clear_locks failed")

class Uploader():
  def __init__(self, dongle_id, root):
    self.dongle_id = dongle_id
    self.api = Api(dongle_id)
    self.root = root

    self.upload_thread = None

    self.last_resp = None
    self.last_exc = None

    self.immediate_priority = {"qlog.bz2": 0}
    self.high_priority = {"rlog.bz2": 0, "fcamera.hevc": 1, "dcamera.hevc": 2}

  def clean_dirs(self):
    try:
      for logname in os.listdir(self.root):
        path = os.path.join(self.root, logname)
        # remove empty directories
        if not os.listdir(path):
          os.rmdir(path)
    except OSError:
      print("clean_dirs failed")

  def get_upload_sort(self, name):
    if name in self.immediate_priority:
      return self.immediate_priority[name]
    if name in self.high_priority:
      return self.high_priority[name] + 100
    return 1000

  def gen_upload_files(self):
    if not os.path.isdir(self.root):
      return
    for logname in listdir_by_creation(self.root):
      path = os.path.join(self.root, logname)
      try:
        names = os.listdir(path)
      except OSError:
        continue
      if any(name.endswith(".lock") for name in names):
        continue

      for name in sorted(names, key=self.get_upload_sort):
        key = os.path.join(logname, name)
        fn = os.path.join(path, name)

        yield (name, key, fn)

  def next_file_to_upload(self, with_raw):
    unprocessed_files = list(self.gen_upload_files())
    upload_files = []

    # remove dcamera files
    if not dcamera_upload:
      for name, key, fn in unprocessed_files:
        if not name == "dcamera.hevc":
          upload_files.append((name, key, fn))
      unprocessed_files = upload_files
      upload_files = []

    # remove files that has .uploaded duplicates
    for name, key, fn in unprocessed_files:
      filename, extension = os.path.splitext(fn)
      if not extension == ".uploaded":
        uploaded_name = fn + ".uploaded"
        if not os.path.isfile(uploaded_name):
          upload_files.append((name, key, fn))

    # try to upload qlog files first
    for name, key, fn in upload_files:
      if name in self.immediate_priority:
        return (key, fn)

    if with_raw:
      # then upload the full log files, rear and front camera files
      for name, key, fn in upload_files:
        if name in self.high_priority:
          return (key, fn)

      # then upload other files
      for name, key, fn in upload_files:
        if not name.endswith('.lock') and not name.endswith(".tmp"):
          return (key, fn)

    return None

  def do_upload(self, key, fn):
    try:
      url_resp = self.api.get("v1.3/"+self.dongle_id+"/upload_url/", timeout=10, path=key, access_token=self.api.get_token())
      print("v1.3/"+self.dongle_id+"/upload_url/")
      print(self.api.get_token())
      url_resp_json = json.loads(url_resp.text)
      print(url_resp_json)
      url = url_resp_json['url']
      headers = url_resp_json['headers']
      print("upload_url v1.3 %s %s" % (url, str(headers)))

      if fake_upload:
        print("*** WARNING, THIS IS A FAKE UPLOAD TO %s ***" % url)
        class FakeResponse():
          def __init__(self):
            self.status_code = 200
        self.last_resp = FakeResponse()
      else:
        with open(fn, "rb") as f:
          self.last_resp = requests.put(url, data=f, headers=headers, timeout=10)
    except Exception as e:
      self.last_exc = (e, traceback.format_exc())
      raise

  def normal_upload(self, key, fn):
    self.last_resp = None
    self.last_exc = None

    try:
      self.do_upload(key, fn)
    except Exception:
      pass

    return self.last_resp

  def upload(self, key, fn):
    try:
      sz = os.path.getsize(fn)
    except OSError:
      print("upload: getsize failed")
      return False

    print("upload: key=%s, fn=%s, sn=%s" % (key, fn, sz))

    print("checking %s with size %s" % (key, sz))

    if sz == 0:
      # can't upload files of 0 size
      os.unlink(fn) # delete the file
      success = True
    else:
      print("uploading %r", fn)
      stat = self.normal_upload(key, fn)
      if stat is not None and stat.status_code in (200, 201):
        print("upload_success: key=%s, fn=%s, sz=%s" % (key, fn, sz))

        # delete the file
        try:
          if delete_after_upload:
            os.unlink(fn)
          else:
            uploaded_name = fn + ".uploaded"
            open(uploaded_name, 'a').close()
        except OSError:
          print("delete_failed: stat=%s, exc=%s, key=%s, fn=%s, sz=%s" % (stat, self.last_exc, key, fn, sz))

        success = True
      else:
        print("upload_failed: stat=%s, exc=%s, key=%s, fn=%s, sz=%s" % (stat, self.last_exc, key, fn, sz))
        success = False

    self.clean_dirs()

    return success

def uploader_fn(exit_event):
  print("uploader_fn")

  if dongle_id is None:
    print("uploader missing dongle_id")
    raise Exception("uploader can't start without dongle id")

  uploader = Uploader(dongle_id, ROOT)

  backoff = 0.1
  while True:
    allow_raw_upload = True
    should_upload = True

    if exit_event.is_set():
      return

    d = uploader.next_file_to_upload(with_raw=allow_raw_upload and should_upload)
    if d is None:
      print("No file to upload, quit.")
      quit()

    key, fn = d

    print("to upload: key=%s, fn=%s" % (key, d))
    success = uploader.upload(key, fn)
    if success:
      backoff = 0.1
    else:
      print("backoff %s" % backoff)
      time.sleep(backoff + random.uniform(0, backoff))
      backoff = min(backoff*2, 120)
    print("upload done, success=%s" % success)

def main(gctx=None):
  uploader_fn(threading.Event())

if __name__ == "__main__":
  main()
