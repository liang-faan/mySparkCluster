
val rawUserArtistData = sc.textFile("file:///opt/workspace/data/user_artist_data_small.txt")

rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
rawUserArtistData.map(_.split(' ')(1).toDouble).stats()

val rawArtistData = sc.textFile("file:///opt/workspace/data/artist_data_small.txt")
val artistByID = rawArtistData.flatMap { line =>
  val(id, name) = line.span(_ != '\t')
  if (name.isEmpty) {
    None
  } else {
    try {
      Some((id.toInt, name.trim))
    } catch {
      case e: NumberFormatException => None
    }
  }
}

val rawArtistAlias = sc.textFile("file:///opt/workspace/data/artist_alias_small.txt")
val artistAlias = rawArtistAlias.flatMap { line =>
  val tokens = line.split('\t')
  if (tokens(0).isEmpty) {
    None
  } else {
    Some((tokens(0).toInt, tokens(1).toInt))
  }
}.collectAsMap()


import org.apache.spark.mllib.recommendation._

val bArtistAlias = sc.broadcast(artistAlias)

val trainData = rawUserArtistData.map { line =>
  val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
  val finalArtistID =
    bArtistAlias.value.getOrElse(artistID, artistID)
  Rating(userID, finalArtistID, count)
}.cache()

val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).
  filter { case Array(user,_,_) => user.toInt == 2064012}

val existingProducts = 
  rawArtistsForUser.map { case Array(_, artist,_) => artist.toInt }.collect().toSet

artistByID.filter { case (id, name) =>
  existingProducts.contains(id)
}.values.collect().foreach(println)

val recommandations = model.recommendProducts(2064012, 5)
recommandations.foreach(println)


val recommendedProductIDs = recommandations.map(_.product).toSet

artistByID.filter { case(id, name) =>
  recommendedProductIDs.contains(id)
}.values.collect().foreach(println)


