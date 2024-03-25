// Task 3ii

db.credits.aggregate([
    // TODO: Write your query here
    //get movies with anderson as director
  {$unwind: "$crew"},
  {$match: {"crew.id": 5655}},
  {$match: {"crew.job": "Director"}},
  {$unwind: "$cast"},
  {$group: {
    _id: "$cast.id",
    name: {$first: "$cast.name"},
    count: {$sum: 1}
  }},
  {$sort: {count: -1, _id: 1}},
  {$limit: 5},
  {$project: {
    _id: 0,
    count: "$count",
    id: "$_id",
    name: "$name"
  }}
])
