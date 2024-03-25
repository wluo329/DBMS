// Task 2ii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    //convert string into array of words
    {$project: {tagline: {$split: ["$tagline", " "]}}},
    //separate words into separate documents
    {$unwind: "$tagline"},
    //lowercase everything
    {$project: {tagline: {$toLower: "$tagline"}}},
    //get rid of punctuation
    {$project: {tagline: {$trim: {input: "$tagline", chars: ".,?! "}}}},
    //filter words 3 or less characters
    {$match: {$expr: {$gt: [{$strLenCP: "$tagline"}, 3]}}},
    {$group: {_id: "$tagline", count: {$sum: 1}}},
    {$sort: {count: -1}},
    {$limit: 20},
    {$project: {_id: 1, count: 1}}
]);
