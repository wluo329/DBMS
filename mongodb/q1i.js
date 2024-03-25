// Task 1i

db.keywords.aggregate([
    // TODO: Write your query here
    {$match: {
        $or: [
            {keywords: {$elemMatch: {name: "mickey mouse"}}},
            {keywords: {$elemMatch: {name: "marvel comic"}}}
        ]
    }},
    {$sort: {movieId: 1}},
    {$project: {_id: 0, movieId: 1}} // clean up output

]);