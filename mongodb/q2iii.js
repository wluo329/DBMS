// Task 2iii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    {$group: {_id: {$cond: [{$and: [
              {$ne: ["$budget", false]},
              {$ne: ["$budget", null]},
              {$ne: ["$budget", ""]},
              {$ne: ["$budget", undefined]}
            ]},
            //round
          {$round: [
            {$cond: {
              if: {$eq: [{$type: "$budget"}, "string"]},
              then: {$toInt: {$trim: {input: "$budget", chars: " $USD"}}},
              else: "$budget"
            }},
            //nearest 10 millionth
            -7
          ]},

          //if not num, do this
           "unknown"]}, count: {$sum: 1}}},
    {$sort: {_id: 1}},
    {$project: {_id: 0, budget: "$_id", count: 1}}
]);




