//default path for our project files
path = 'C:\\Users\\haris\\Development\\701'

//create a new neo4j graph at defaule location, titled "Ratings"
g = new Neo4jGraph(path + '\\graph\\Ratings')

//remove the default edge which is not required.
g.dropIndex("edges")

//set the transaction buffer for the graph database to 1000 mutations per commit.
g.setMaxBufferSize(1000)

//path to the dataset
dataset = path + '\\dataset'

//a map of OccupationIDs:OccupationNames, which will be helpful later.
occupations = [0:'other', 1:'academic/educator', 2:'artist',
  3:'clerical/admin', 4:'college/grad student', 5:'customer service',
  6:'doctor/health care', 7:'executive/managerial', 8:'farmer',
  9:'homemaker', 10:'K-12 student', 11:'lawyer', 12:'programmer',
  13:'retired', 14:'sales/marketing', 15:'scientist', 16:'self-employed',
  17:'technician/engineer', 18:'tradesman/craftsman', 19:'unemployed', 20:'writer']


/**********************
Setting up the database
***********************/

/*
movies.dat
...
2623::Trippin' (1999)::Comedy
2624::After Life (1998)::Drama
2625::Black Mask (Hak hap) (1996)::Action
...

Go through movies.dat and create a vertex for 
1. Every new movie and 
2. Every new genre.

How? 
for each line in the file
	split the line into tokens
	create a new movie vertex with name
	create a new genre vertex with genre
	add an edge from the movie to the genre.

TODO. "Add year as a field in the movie"
*/
new File(dataset + '\\movies.dat').eachLine {def line ->
  def components = line.split('::');
  def movieVertex = g.addVertex(['type':'Movie', 'movieId':components[0].toInteger(), 'title':components[1]]);
  components[2].split('\\|').each { def genera ->
    def hits = g.idx(T.v)[[genera:genera]].iterator();
    def generaVertex = hits.hasNext() ? hits.next() : g.addVertex(['type':'Genera', 'genera':genera]);
    g.addEdge(movieVertex, generaVertex, 'hasGenera');
  }
}

/*
users.dat
userId, gender, age, occupation, zipcode
1::F::1::10::48067
2::M::56::16::70072
3::M::25::15::55117
...

Go through users.dat and create a vertex for 
1. Every new user
2. Every occupation

add an edge from user to occupation.

*/
new File(dataset + '\\users.dat').eachLine {def line ->
  def components = line.split('::');
  def userVertex = g.addVertex(['type':'User', 'userId':components[0].toInteger(), 'gender':components[1], 'age':components[2].toInteger()]);
  def occupation = occupations[components[3].toInteger()];
  def hits = g.idx(T.v)[[occupation:occupation]].iterator();
  def occupationVertex = hits.hasNext() ? hits.next() : g.addVertex(['type':'Occupation', 'occupation':occupation]);
  g.addEdge(userVertex, occupationVertex, 'hasOccupation');
}

/*
ratings.dat
userId, movieId, stars (1-5 rating scale), and timestamp. 
...
1::1287::5::978302039
1::2804::5::978300719
1::594::4::978302268
...

Go through ratings.dat and add an edge from userId to movie.
Add the 'stars' property to the edge, giving it the appropriate count


*/
new File(dataset + '\\ratings.dat').eachLine {def line ->
  def components = line.split('::');
  def ratedEdge = g.addEdge(g.idx(T.v)[[userId:components[0].toInteger()]].next(), g.idx(T.v)[[movieId:components[1].toInteger()]].next(), 'rated');
  ratedEdge.setProperty('stars', components[2].toInteger());
}

g.stopTransaction(TransactionalGraph.Conclusion.SUCCESS)


/**********************
Traversing the graph
***********************/

//number of total vertices:
g.V().count()
//number of user vertices:
g.V[[type:'User']].count()
//number of female user vertices
g.V[[type:'User']][[gender:'F']].count()
// Get vertex by index 1085
g.V().getAt(1085).map 


/************************************************************
Collaborative Filtering : Starting with a user with userID 1
*************************************************************/

//get a user by id
u = g.idx(T.v)[[userId:1]] >> 1
//alternate:
//u = g.V[[type:'User']][[userId:6032]]

//all the edges with ratings greater than 3
u.outE('rated').filter{it.getProperty('stars') > 3}.inV()

// Edges for top rated movies for userID 6032
//g.V[[type:'User']][[userId:6032]].outE('rated').filter{it.getProperty('stars') > 3}

// Top rated movies by this user
movies_count = g.V[[type:'User']][[userId:6032]].outE('rated').filter{it.getProperty('stars') > 3}.inV().count()
movies = g.V[[type:'User']][[userId:6032]].outE('rated').filter{it.getProperty('stars') > 3}.inV() >> 82


/**********************************************************
Collaborative Filtering : Starting with a movie "Toy Story"
***********************************************************/
//get reference to 'Toy Story (1995)'
v = g.idx(T.v)[[title:'Toy Story (1995)']] >> 1

/*
Define a step "corated(x)" which does the following:
Start from a movie (eg. "Toy Story (1995)") 
Get the incoming "rated" edges — inE(‘rated’)
Filter out those edges whose star property is less than x — filter{it.getProperty(‘stars’) > x}
Get the tail user vertices of the remaining edges — outV
Get the rating edges of those user vertices — outE(‘rated’)
Filter out those edges whose star property is less than 4 — filter{it.getProperty(‘stars’) > 3}
Get the head movie vertices of the remaining edges — inV
*/
Gremlin.defineStep('corated',[Vertex,Pipe], { def stars ->
  _().inE('rated').filter{it.getProperty('stars') > stars}.outV.outE('rated').filter{it.getProperty('stars') > stars}.inV})

/*
Get all the movies corated with Toy Story — v.corated(3)
There will be many high rated paths from "Toy Story" to other movies, many (most) of these paths will lead to the same movies.
It is possible to use these duplicates as a ranking mechanism–ultimately, a recommendation: 
The higher number of paths to a movie starting from Toy Story, the higher the movie is corated to "Toy Story".

For this, use a map, which stores the movies against the count of the number of paths to the movie from Toy Story.
Sort the map by the count and return that as the recommendation list.
Remove the reflexive paths by filtering out "Toy Story" itself, from the results — filter{it != v}
*/
map = [:]  
v.corated(3).filter{it != v}.title.groupCount(map) >> -1
map.sort{a,b -> b.value <=> a.value}[0..9] 

/*
Print the values
i = 10
for ( e in map ) {
    println "movie = ${e.key}, count = ${e.value}"
    i--;
    if(i < 0) 
    	break
}
*/
	
g.stopTransaction(TransactionalGraph.Conclusion.SUCCESS)