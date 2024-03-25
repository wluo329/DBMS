// Task 2i


db.movies_metadata.aggregate([
  // TODO: Write your query here
  {$match: {vote_count: {$gte: 1838}}},
  {$project: {_id: 0, title: 1, vote_count: 1, score: {$round: [{$add: [{$multiply: [{$divide: ["$vote_count", {$add: ["$vote_count", 1838]}]},"$vote_average" ]}, {$multiply: [{$divide: [1838, {$add: ["$vote_count", 1838]}]}, 7]}]}, 2]}}},
  {$sort: {score: -1, vote_count: -1, title: 1}},
  {$limit: 20},
  {$project: {_id: 0, title: 1, vote_count: 1, score: 1}}
]);





