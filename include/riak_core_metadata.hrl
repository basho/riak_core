-type metadata_prefix()     :: {binary() | atom(), binary() | atom()}.
-type metadata_key()        :: any().
-type metadata_pkey()       :: {metadata_prefix(), metadata_key()}.
-type metadata_value()      :: any().
-type metadata_tombstone()  :: '$deleted'.
-type metadata_resolver()   :: fun((metadata_value() | metadata_tombstone(),
                                    metadata_value() | metadata_tombstone()) -> metadata_value()).
-type metadata_modifier()   :: fun(([metadata_value() | metadata_tombstone()] | undefined) ->
                                          metadata_value()).
-type metadata_object()     :: {metadata, dvvset:clock()}.
-type metadata_context()    :: dvvset:vector().

-record(metadata_broadcast, {
          pkey  :: metadata_pkey(),
          obj   :: metadata_object()
         }).
-type metadata_broadcast()  ::  #metadata_broadcast{}.
