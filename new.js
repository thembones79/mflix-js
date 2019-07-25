var pipeline = [
  {
    $match: {
      "imdb.rating": { $gte: 7 },
      $or: [{ rated: "PG" }, { rated: "G" }],
      languages: { $all: ["English", "Japanese"] },
      $and: [{ genres: { $ne: "Crime" } }, { genres: { $ne: "Horror" } }],
    },
  },
  {
    $project: {
      _id: 0,
      title: 1,
      rated: 1,
    },
  },
]

var newPipe = [
  {
    $project: {
      titleWordCount: { $size: { $split: ["$title", " "] } },
    },
  },
  { $match: { titleWordCount: 1 } },
]

pipeline = [
  {
    $match: {
      writers: { $elemMatch: { $exists: true } },
      cast: { $elemMatch: { $exists: true } },
      directors: { $elemMatch: { $exists: true } },
    },
  },
  {
    $project: {
      writers: {
        $map: {
          input: "$writers",
          as: "writer",
          in: {
            $arrayElemAt: [{ $split: ["$$writer", " ("] }, 0],
          },
        },
      },
      cast: 1,
      directors: 1,
      title: 1,
    },
  },
  {
    $project: {
      _id: 0,
      title: 1,
      laborOfLove: {
        $gt: [
          { $size: { $setIntersection: ["$writers", "$cast", "$directors"] } },
          0,
        ],
      },
    },
  },
  { $match: { laborOfLove: true } },
]

var favorites = [
  "Sandra Bullock",
  "Tom Hanks",
  "Julia Roberts",
  "Kevin Spacey",
  "George Clooney",
]

pipeline = [
  {
    $match: {
      "tomatoes.viewer.rating": { $gte: 3 },
      countries: { $in: ["USA"] },
      cast: { $exists: true },
    },
  },
  {
    $addFields: {
      num_favs: { $size: { $setIntersection: ["$cast", favorites] } },
    },
  },
  {
    $sort: {
      num_favs: -1,
      "tomatoes.viewer.rating": -1,
      title: -1,
    },
  },
  { $skip: 24 },
  { $limit: 1 },
  { $project: { title: 1 } },
]

pipeline = [
  {
    $match: {
      "imdb.rating": { $gte: 1 },
      "imdb.votes": { $gte: 1 },
      year: { $gte: 1990 },
    },
  },
  {
    $addFields: {
      scaledVotes: {
        $add: [
          1,
          {
            $multiply: [
              9,
              {
                $divide: [
                  { $subtract: ["$imdb.votes", 5] },
                  { $subtract: [1521105, 5] },
                ],
              },
            ],
          },
        ],
      },
    },
  },
  {
    $addFields: {
      normalized_rating: {
        $avg: ["$imdb.rating", "$scaledVotes"],
      },
    },
  },
  {
    $sort: { normalized_rating: 1 },
  },
  { $limit: 1 },
]








pipeline = [
  { $match: { awards: { $regex: /Won \d+ Oscar/ } } },
  {
    $group: {
      _id: null,
      highest_rating: { $max: "$imdb.rating" },
      lowest_rating: { $min: "$imdb.rating" },
      average_rating: { $avg: "$imdb.rating" },
      deviation: { $stdDevSamp: "$imdb.rating" },
    },
  },
]


pipeline = [
    {$unwind:"$cast"},
    {$group: {
        "_id": "$cast",
        "numFilms": {$sum:1},
        "average":{$avg:"$imdb.rating"}
    }}, 
    {$sort: {"numFilms":-1}},
    {$limit:1}
]


ass = [
    { $unwind : "$airlines" },
    { $lookup: {
            from: "air_routes",
            localField: "airlines",
            foreignField: "airline.name",
            as: "routes"
        }
    },
    { $unwind : "$routes" },
	{ $match : { "routes.airplane" : { $in : [ "747", "380" ] } } },
    { $group : {
		"_id" : "$name",
		"routes_count" : { $sum : 1 } 
	    }
	},
	{ $sort : {"routes_count" : -1 } }
];


pipeline = [
  {
    $match: {
      "imdb.rating": { $gt: 0 },
      metacritic: { $gt: 0 },
    },
  },
  {
    $facet: {
      topTenImdb: [{ $sort: { "imdb.rating": -1 } }, { $limit: 10 }],
      topTenMetacritic: [{ $sort: { metacritic: -1 } }, { $limit: 10 }],
    },
  },
  {
    $project: {
      commonTopFilms: {
        $size: {
          $setIntersection: ["$topTenImdb", "$topTenMetacritic"],
        },
      },
    },
  },
]