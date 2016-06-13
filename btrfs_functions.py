#!/usr/bin/python
'''
Created on 13 Jun 2016

@author: Maximilian Mehnert <maximilian.mehnert@gmx.de>
'''

import subprocess
import datetime
import signal
import time
import pprint
import os.path
pp = pprint.PrettyPrinter(indent=4)





class btrfs_fs:
	fs=None
	destructive=False
	snapshots=[]
	remote_cmd=""
	dry_run=False
	destructive=False

	def __str__(self):
		return self.remote_cmd+" FS:"+self.fs

	def __init__(self,fs=None,remote_cmd="", verbose=False, dry_run=False, destructive=False):
		self.verbose=verbose
		self.dry_run=dry_run
		self.remote_cmd=remote_cmd
		self.dry_run=dry_run
		self.destructive=destructive

		if fs==None:
			raise ValueError("No filesystem specified")
		else:
			self.fs=fs
		self.update_snapshots()

	def update_snapshots(self):
		snapshot_list=[]
		command=self.remote_cmd+" btrfs subvolume list -o --sort=ogen "+self.fs+"|awk '{print $NF}' "
#		print("update_snapshots: "+command)
		for snapshot in subprocess.check_output("set -o pipefail; "+command, shell=True,universal_newlines=True, executable='/bin/bash', stderr=subprocess.STDOUT).split("\n"):
#			print("fs: "+self.fs+" snap: "+snapshot+" dirname: "+os.path.dirname(snapshot)+" base: "+os.path.dirname(snapshot))
			if self.fs.endswith(os.path.dirname(snapshot)) and os.path.dirname(snapshot)!="":
#				print("adding "+snapshot)
				snapshot_list.append(os.path.basename(snapshot))
		self.snapshots=snapshot_list
		return self.snapshots


	def get_snapshots(self):
		return self.snapshots

	def get_snapshots_reversed(self):
		return reversed(self.snapshots)

	def get_last_snapshot(self):
		return self.snapshots[-1]

	def get_first_snapshot(self):
		return snapshots[0]

	def get_last_common_snapshot(self,dst_fs=None):
		for dst_snapshot in dst_fs.get_snapshots_reversed():
			if dst_snapshot in self.get_snapshots():
				return dst_snapshot
		return None

	def create_snapshot(self,source_dir=None, postfix=""):
		if len(postfix)==0:
			raise ValueError("postfix for snapshot must be defined")
		snapshot=self.timestamp_string()+"-"+postfix
		snapshot_command=self.remote_cmd+" btrfs subvolume snapshot -r "+source_dir+" "+self.fs+"/"+snapshot
		if self.verbose or self.dry_run:
			print("Running: "+snapshot_command)
		if not self.dry_run:
			subprocess.check_call(snapshot_command, shell=True, stderr=subprocess.STDOUT)
		self.snapshots.append(snapshot)
#		print("updated snapshots: "+str(self.snapshots))
		return snapshot


	def transfer_to(self,dst_fs=None):
		if self.verbose:
			print("trying to transfer: "+self.remote_cmd+" "+self.fs+" to "+dst_fs.remote_cmd+" "+dst_fs.fs+".")
		command=self.remote_cmd+" btrfs send "+self.fs+"/"+self.snapshots[0]+"|"+dst_fs.remote_cmd+" btrfs receive "+dst_fs.fs
		if self.verbose or self.dry_run:
			print("running "+command)
		if not self.dry_run:
			subprocess.call("set -o pipefail; "+command, shell=True, executable='/bin/bash', stderr=subprocess.STDOUT)
			dst_fs.update_snapshots()
			self.sync_with(dst_fs=dst_fs)
		return True



	def sync_with(self,dst_fs=None):
		if self.verbose:
			print("Syncing "+self.remote_cmd+" "+self.fs+" TO "+dst_fs.remote_cmd+" "+dst_fs.fs+".")

		last_common_snapshot=self.get_last_common_snapshot(dst_fs=dst_fs)

		if last_common_snapshot != None:
			return self.run_sync(dst_fs=dst_fs,start_snap=last_common_snapshot)
		else:
			return self.transfer_to(dst_fs=dst_fs)

	def run_sync(self,dst_fs=None, start_snap=None):
		parent=start_snap
		for snap in self.snapshots[self.snapshots.index(start_snap)+1:]:
			sync_command=self.remote_cmd+" btrfs send -p "+self.fs+"/"+parent+" "+self.fs+"/"+snap+"|"+dst_fs.remote_cmd+" btrfs receive "+dst_fs.fs

			if self.verbose or self.dry_run:
				print("Running sync: "+sync_command)
			if not self.dry_run:
				subprocess.call("set -o pipefail; "+sync_command, shell=True, executable='/bin/bash', stderr=subprocess.STDOUT)

				dst_fs.update_snapshots()
				if snap in dst_fs.get_snapshots():
					if self.verbose:
						print("Sucessfully transferred "+snap)
				else:
					raise Exception ( "sync : "+sync_command+" failed. "+snap+" is not in "+str(dst_fs.get_snapshots()))
			parent=snap
		return True

	def timestamp_string(self):
		return datetime.datetime.today().strftime("%F--%H-%M-%S")

	def destroy_snapshot(self,snapshot):
		snapshot_command=self.remote_cmd+" btrfs subvolume delete "+self.fs+"/"+snapshot
		if self.verbose or self.dry_run:
			print("Running: "+snapshot_command)
		if not self.dry_run:
			subprocess.check_call(snapshot_command, shell=True, stderr=subprocess.STDOUT)

	def clean_snapshots(self,postfix="", number_to_keep=None):
		if self.verbose == True:
			print("clean_snapshots:"+str(self)+": "+postfix)
		snapshot_list=[]
		for snapshot in self.get_snapshots():
			if snapshot.endswith(postfix):
				snapshot_list.append(snapshot)

		number_to_remove= len(snapshot_list)-number_to_keep
		if number_to_remove >0:
			for snap_to_remove in snapshot_list[:number_to_remove]:
				self.destroy_snapshot(snap_to_remove)

	def clean_other_snapshots(self,postfixes_to_ignore=[], number_to_keep=None):
		if self.verbose == True:
			print("clean_other_snapshots:"+str(self))
		snapshot_list=[]
		for snapshot in self.get_snapshots():
			skip=False
			for postfix in postfixes_to_ignore:
				if snapshot.endswith(postfix):
					skip=True
					break
			if skip==False:
				snapshot_list.append(snapshot)

		number_to_remove= len(snapshot_list)-number_to_keep
		if number_to_remove >0:
			for snap_to_remove in snapshot_list[:number_to_remove]:
				self.destroy_snapshot(snap_to_remove=snap_to_remove)
