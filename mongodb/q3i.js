// Task 3i

db.credits.aggregate([
    // TODO: Write your query here
    {$unwind: "$cast"},
    {$match: {"cast.id": 7624}},
    {$lookup: {
      from: "movies_metadata",
      localField: "movieId",
      foreignField: "movieId",
      as: "metadata"
    }},

    {$project: {
        _id: 0,
        title: {$arrayElemAt: ["$metadata.title", 0]},
        release_date: {$arrayElemAt: ["$metadata.release_date", 0]},
        character: "$cast.character"
      }},
    {$sort: {release_date: -1}}
])

