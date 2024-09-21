library(RMySQL)
library(igraph)

# 2. Settings
db_user <- 'root'
db_password <- 'root'
db_name <- 'thesis'
db_host <- 'localhost' # for local access
db_port <- 3306


mydb <-  dbConnect(MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)





create_net <- function(q1, q2){
  q1 <- dbSendQuery(mydb, q1)
  q1 <-  fetch(q1, n = -1)
  
  g<-graph_from_data_frame(q1, directed = T, vertices=NULL)

  q2 <- dbSendQuery(mydb, q2)
  q2 <-  fetch(q2, n = -1)  
  
  new_nodes<-setdiff(q2 ,V(g)$id)
  g<-add_vertices(g,nv = length(setdiff(q2$author_id,V(g)$name)),name=setdiff(q2$author_id,V(g)$name))
  return (g)
}

print_network_measurements <- function(graph) {
  # Ensure the input is an igraph object
  if (!inherits(graph, "igraph")) {
    stop("Input must be an igraph object")
  }
  
  cat("Network Information:\n")
  cat("Number of nodes: ", vcount(graph), "\n")
  cat("Number of edges: ", ecount(graph), "\n")
  
  density <- edge_density(graph)
  cat("Density: ", density, "\n")
  
  # weighted density
  edge_weights <- E(graph)$weight
  if (length(edge_weights) > 0) {
    weighted_density <- sum(edge_weights) / (vcount(graph) * (vcount(graph) - 1) / 2)
    cat("Weighted density: ", weighted_density, "\n")
  } else {
    cat("Weighted density: Not applicable (no edge weights)\n")
  }
  
  cat("Diameter: ", diameter(graph, unconnected = TRUE), "\n")
  cat("Average degree: ", mean(degree(graph, mode = "in")), "\n")
  cat("Average strength: ", mean(strength <- strength(graph, mode="in")), "\n")
  
  # isolated nodes
  in_degrees <- degree(graph, mode="in")
  out_degrees <- degree(graph, mode="out")
  total_degrees <- in_degrees + out_degrees
  zero_total_degree_nodes <- V(graph)[total_degrees == 0]
  num_zero_total_degree_nodes <- length(zero_total_degree_nodes)
  cat("Isolated nodes: ", num_zero_total_degree_nodes, "\n")
  
  # average path length
  cat("Average path length: ", mean_distance(graph), "\n")

  # clustering coefficient
  cat("Clustering coefficient: ", transitivity(graph, type = "global"), "\n")
  
  # In-Degree Centralization
  cat("In-degree centralization: ", centralization.degree(graph, mode = "in")$centralization, "\n")
  
  # Out-Degree Centralization
  cat("Out-degree centralization: ", centralization.degree(graph, mode = "out")$centralization, "\n")
  
  # Betweenness Centralization
  cat("Betweenness centralization: ", centralization.betweenness(graph)$centralization, "\n")
  
  # Weakly-Connected Components
  weakly_connected <- components(graph, mode = "weak")
  num_weakly_connected_components <- length(unique(weakly_connected$membership))
  cat("Number of weakly-connected components: ", num_weakly_connected_components, "\n")
  
  # Strongly-Connected Components
  strongly_connected <- components(graph, mode = "strong")
  num_strongly_connected_components <- length(unique(strongly_connected$membership))
  cat("Number of strongly-connected components: ", num_strongly_connected_components, "\n")
  
}


#networks
g_a1 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                WHERE r.time_to_retweet <= 1662.81 AND t.created_at <= '2022-12-09 17:00:00' 
                AND original_author_id IS NOT NULL 
                GROUP BY r.author_id, r.original_author_id;",
                "SELECT author_id  
                FROM en_tweets  
                WHERE created_at <= '2022-12-09 17:00:00';")
vcount(g_a1)
ecount(g_a1)

g_b1 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet > 1662.81 AND r.time_to_retweet <= 16628.1 
                   AND t.created_at <= '2022-12-09 17:00:00' 
                   GROUP BY r.author_id, r.original_author_id;", 
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at <= '2022-12-09 17:00:00' ;")
vcount(g_b1)
ecount(g_b1)



g_c1 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet > 16628.1 AND t.created_at <= '2022-12-09 17:00:00' 
                   GROUP BY r.original_author_id, r.author_id;", 
                   "SELECT author_id  
                   FROM en_tweets 
                   WHERE created_at <= '2022-12-09 17:00:00';")
vcount(g_c1)
ecount(g_c1)


g_a2 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet <= 1662.81 AND t.created_at>'2022-12-09 17:00:00' and t.created_at<='2022-12-16 17:00:00' 
                   GROUP BY r.author_id, r.original_author_id;",
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-09 17:00:00' and created_at<='2022-12-16 17:00:00';")
vcount(g_a2)
ecount(g_a2)


g_b2 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet > 1662.81 AND r.time_to_retweet <= 16628.1 
                   AND t.created_at>'2022-12-09 17:00:00' and t.created_at<='2022-12-16 17:00:00' 
                   GROUP BY r.author_id, r.original_author_id;",
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-09 17:00:00' and created_at<='2022-12-16 17:00:00'  ;")
vcount(g_b2)
ecount(g_b2)


g_c2 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet > 16628.1  AND t.created_at>'2022-12-09 17:00:00' and t.created_at<='2022-12-16 17:00:00' 
                   GROUP BY r.original_author_id, r.author_id;",
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-09 17:00:00' and created_at<='2022-12-16 17:00:00';")
vcount(g_c2)
ecount(g_c2)



g_a3 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet <= 1662.81 AND t.created_at>'2022-12-16 17:00:00' and t.created_at<='2022-12-23 17:00:00' 
                   GROUP BY r.author_id, r.original_author_id;", 
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-16 17:00:00' and created_at<='2022-12-23 17:00:00';")
vcount(g_a3)
ecount(g_a3)


g_b3 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet > 1662.81 AND r.time_to_retweet <= 16628.1 
                   AND t.created_at>'2022-12-16 17:00:00' and t.created_at<='2022-12-23 17:00:00' 
                   GROUP BY r.author_id, r.original_author_id;", 
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-16 17:00:00' and created_at<='2022-12-23 17:00:00';")
vcount(g_b3)
ecount(g_b3)


g_c3 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet > 16628.1  AND t.created_at>'2022-12-16 17:00:00' and t.created_at<='2022-12-23 17:00:00' 
                   GROUP BY r.original_author_id, r.author_id;", 
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-16 17:00:00' and created_at<='2022-12-23 17:00:00';")
vcount(g_c3)
ecount(g_c3)


g_a4 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet <= 1662.81 AND t.created_at>'2022-12-23 17:00:00' and t.created_at<='2022-12-30 17:00:00' 
                   GROUP BY r.author_id, r.original_author_id;", 
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-23 17:00:00' and created_at<='2022-12-30 17:00:00';")
vcount(g_a4)
ecount(g_a4)


g_b4 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet > 1662.81 AND r.time_to_retweet <= 16628.1 
                   AND t.created_at>'2022-12-23 17:00:00' and t.created_at<='2022-12-30 17:00:00' 
                   GROUP BY r.author_id, r.original_author_id;", 
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-23 17:00:00' and created_at<='2022-12-30 17:00:00';")
vcount(g_b4)
ecount(g_b4)


g_c4 <- create_net("SELECT r.author_id as source, r.original_author_id as target, COUNT(*) as weight 
                   FROM en_retweets r join en_tweets t on t.id=r.original_tweet_id 
                   WHERE r.time_to_retweet > 16628.1  AND t.created_at>'2022-12-23 17:00:00' and t.created_at<='2022-12-30 17:00:00' 
                   GROUP BY r.original_author_id, r.author_id;", 
                   "SELECT author_id  
                   FROM en_tweets  
                   WHERE created_at>'2022-12-23 17:00:00' and created_at<='2022-12-30 17:00:00';")
vcount(g_c4)
ecount(g_c4)




write_graph(g_a1, file = "1a", format = "pajek")
write_graph(g_a1g, file = "1a.net", format = "pajek")


print_network_measurements(g_a1)