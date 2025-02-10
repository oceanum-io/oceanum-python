oceanum.storage.FileSystem
==========================

.. currentmodule:: oceanum.storage

.. autoclass:: FileSystem

   
   
   .. rubric:: Attributes

   .. autosummary::
   
      ~FileSystem.async_impl
      ~FileSystem.blocksize
      ~FileSystem.cachable
      ~FileSystem.disable_throttling
      ~FileSystem.fsid
      ~FileSystem.loop
      ~FileSystem.mirror_sync_methods
      ~FileSystem.protocol
      ~FileSystem.root_marker
      ~FileSystem.sep
      ~FileSystem.transaction
      ~FileSystem.storage_args
      ~FileSystem.storage_options
   
   

   
   
   .. rubric:: Methods
   
   .. automethod:: __init__
   .. automethod:: cat
   .. automethod:: cat_file
   .. automethod:: cat_ranges
   .. automethod:: checksum
   .. automethod:: clear_instance_cache
   .. automethod:: close_session
   .. automethod:: copy
   .. automethod:: cp
   .. automethod:: cp_file
   .. automethod:: created
   .. automethod:: current
   .. automethod:: delete
   .. automethod:: disk_usage
   .. automethod:: download
   .. automethod:: du
   .. automethod:: end_transaction
   .. automethod:: exists
   .. automethod:: expand_path
   .. automethod:: find
   .. automethod:: from_dict
   .. automethod:: from_json
   .. automethod:: get
   .. automethod:: get_file
   .. automethod:: get_mapper
   .. automethod:: glob
   .. automethod:: head
   .. automethod:: info
   .. automethod:: invalidate_cache
   .. automethod:: isdir
   .. automethod:: isfile
   .. automethod:: lexists
   .. automethod:: listdir
   .. automethod:: ls
   .. automethod:: makedir
   .. automethod:: makedirs
   .. automethod:: mkdir
   .. automethod:: mkdirs
   .. automethod:: modified
   .. automethod:: move
   .. automethod:: mv
   .. automethod:: open
   .. automethod:: open_async
   .. automethod:: pipe
   .. automethod:: pipe_file
   .. automethod:: put
   .. automethod:: put_file
   .. automethod:: read_block
   .. automethod:: read_bytes
   .. automethod:: read_text
   .. automethod:: rename
   .. automethod:: rm
   .. automethod:: rm_file
   .. automethod:: rmdir
   .. automethod:: set_session
   .. automethod:: sign
   .. automethod:: size
   .. automethod:: sizes
   .. automethod:: start_transaction
   .. automethod:: stat
   .. automethod:: tail
   .. automethod:: to_dict
   .. automethod:: to_json
   .. automethod:: touch
   .. automethod:: ukey
   .. automethod:: unstrip_protocol
   .. automethod:: upload
   .. automethod:: walk
   .. automethod:: write_bytes
   .. automethod:: write_text
   
   