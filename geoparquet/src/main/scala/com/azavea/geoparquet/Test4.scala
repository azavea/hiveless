package com.azavea.geoparquet

object Test4 {
  // we read all geometries
  // build an R tree
  // assign to each leaf an index

  // every block = the leaf of the rtree
  // every rtree branch contains indices for the related blocks to it

  // i.e. we query by geometry; the result of the query is list of blocks that intersect the geom

  // deal

  // how to get the tree leaf if?
  // we can store the in index the position of a group it belongs to

  // how to colocate geometries?
  // groupBy key; compose groups ofr the goruped by key data
  // build the rtree // bulk?

  // dump into the disk

  // the result of a rtree query will be a list of group identifies

}
