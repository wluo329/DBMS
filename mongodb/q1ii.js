// Task 1ii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    {$match: {
        genres: { $elemMatch: {name: "Comedy"}},
        vote_count: {$gte: 50}
        }},

    {$sort: {vote_average: -1, vote_count: -1, movieId: 1}},
    {$limit: 50},
    {$project: {_id: 0, title: 1, vote_average: 1, vote_count: 1, movieId: 1}} // clean up output

]);