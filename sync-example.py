#!/usr/bin/python
'''
Created on 16 Jun 2016

@author: Maximilian Mehnert <maximilian.mehnert@gmx.de>
'''

from	btrfs_functions import *


if __name__ == '__main__':
	verbose=True
	dry_run=False
	transfer_marker_postfix="storage"
	src_base="/root/.btrfs-top-lvl/snapshots/"
	dst_base="/mounts/storage/laptop/"
	for vol in ["root","home"]:
		src_fs=btrfs_fs(fs=src_base+vol, verbose=verbose, dry_run=dry_run)
		dst_fs=btrfs_fs(fs=dst_base+vol, verbose=verbose, dry_run=dry_run, remote_cmd="/usr/bin/ssh -T -o Compression=no nas",)
# make a snapshot of the last snapshot as a reference to this synchronization
		src_fs.create_snapshot(source_dir=src_fs.fs+"/"+src_fs.get_last_snapshot(),postfix=transfer_marker_postfix)

#		print("src_snapshots: "+str(src_fs.snapshots))
#		print("dst_snapshots: "+str(dst_fs.snapshots))
		print("common snapshot: "+(src_fs.get_last_common_snapshot(dst_fs) or "" ))

		if not src_fs.sync_with(dst_fs):
			print("sync failure for "+src_fs.fs)
		else:

			src_fs.clean_snapshots(postfix=transfer_marker_postfix,
													 number_to_keep=2)
			dst_fs.clean_snapshots(postfix=transfer_marker_postfix,
													 number_to_keep=2)
			for tuple in [["5min",0],
										["hourly",12],
										["quarterly",8],
										["daily",7],
										["weekly",4],
										["monthly",12]]:
				dst_fs.clean_snapshots(postfix=tuple[0],
											number_to_keep=tuple[1])

#			print("cleanup:")
#			postfixes_to_ignore=[transfer_marker_postfix]
#			src_fs.clean_other_snapshots(postfixes_to_ignore=postfixes_to_ignore,number_to_keep=10)
