<b>Key - Value Data Store</b>

-> File-based key-value data store that supports the basic CRD (create, read, and delete)
   operations.
   
-> New Key-Value pairs can be added by using create operation. [Key -(String) , Value -(JSON Object)]

-> Every key has an optional Time-To-Live property[Integer defining the number of seconds
   the key must be retained in the data store] when its created.   
  
-> Keys which have a valid Time-To-Live property are maintained in local cache.

-> The Data Store removes the expired keys regularly from cache.

-> Data can be read using read operation and removed using delete operation.


