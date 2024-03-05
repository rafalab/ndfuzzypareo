#' Helper functions for fuzzy match
#'
#' @import data.table
#' @import stringr
#' @import randomForest
#' @import doSNOW
#' @import foreach
#' @importFrom parallel detectCores stopCluster
#' @importFrom stringdist stringdist stringdistmatrix
#' @importFrom matrixStats rowAnyNAs

fix_names <- function(x){
  levels(x) <-
    str_replace_all(levels(x), c("á" = "a", "é" = "e", "í"= "i", "ó" = "o", "ú" = "u", "ü" = "u", "ñ" = "n",
                                 "ph" = "f", "nn"="n", "tt"="t", "ss"="s")) |>
    str_remove_all("^dr\\s+") |>
    str_remove_all("^lcdo\\s+") |>
    str_remove_all("^sor\\s+") |>
    str_replace_all("(?!^)\\s*-\\s*(?!$)", " ") |>
    str_replace_all("\\s+(jr|junior|ii|iii)(\\s+|$)", "\\2") |>
    str_remove_all("[^a-z\\s]+")

  x <- forcats::fct_recode(x, NULL = "")
  return(x)
}
join_dela <- function(x){
  levels(x) <- str_replace_all(levels(x), "^(y|de|la|las|los|del|lo|di|da|le|st|mc|mac|van|san|saint|dos|el|d|o)(\\s+|$)", "\\1") |>
    str_replace_all("\\s+(y|de|la|las|los|del|lo|di|da|le|st|mc|mac|van|san|saint|dos|el|d|o)(\\s+|$)", " \\1") |>
    str_replace_all("^(dela|delas|delos|delo)(\\s+|$)", "\\1") |>
    str_replace_all("\\s+(dela|delas|delos|delo)(\\s+|$)", " \\1")
  return(x)
}

fct_trim <- function(x){
  levels(x) <- str_trim(levels(x))
  return(x)
}
fct_to_lower <- function(x){
  levels(x) <- str_to_lower(levels(x))
  return(x)
}

fct_to_title <- function(x){
  levels(x) <- str_to_title(levels(x))
  return(x)
}
fct_nchar <- function(x){
  nc<- nchar(levels(x))
  nc[as.numeric(x)]
}

compute_name_freqs <- function(tab){ ## a name must appear at least min times to be considered not a mistake
  pn_freq <- tab[!is.na(pn), .N, by = pn][, freq := as.numeric(N)/as.numeric(sum(N))][,!"N"]
  setnames(pn_freq, "pn", "name")
  pn_freq <- pn_freq[order(freq, decreasing = TRUE)]

  sn_freq <- tab[!is.na(sn), .N, by = sn][, freq := as.numeric(N)/as.numeric(sum(N))][!is.na(sn),!"N"]
  setnames(sn_freq, "sn", "name")
  sn_freq <- sn_freq[order(freq, decreasing = TRUE)]

  a_freq <- tab[!is.na(ap), .N, by = ap][, freq := as.numeric(N)/as.numeric(sum(N))][!is.na(ap),!"N"]
  setnames(a_freq, "ap", "name")
  a_freq <- a_freq[order(freq, decreasing = TRUE)]

  return(list(pn = pn_freq, sn = sn_freq, ap = a_freq, am = a_freq))
}

reverse_date <- function(tab){
  tab <- copy(tab)
  tab[, reverse_dob := as.Date(NA)]
  tab[mday(dob)<=12 & mday(dob)!=month(dob),
      reverse_dob :=
      apply(cbind(year(dob),  mday(dob), month(dob)),1,
           function(x) as.Date(paste(x,collapse="-")))]
  tab[, dob:=reverse_dob]
  tab <- tab[, !"reverse_dob"]
  tab[!is.na(dob),]
}

get_all_matches <-  function(query, target, total.max = 8, full.max= 8,
                             check.truncated = TRUE, truncate = "am", self.match = FALSE, max.rows = 100, n.cores = NULL) {
  message("Calculando frecuencias de los nombres.")
  freq <- compute_name_freqs(target)

  query <- copy(query)
  target <- copy(target)

  cols <- c("full", "pn", "sn", "sn_i", "ap", "am")
  query <- query[,(cols) := lapply(.SD, as.character), .SDcols = cols]
  target <- target[,(cols) := lapply(.SD, as.character), .SDcols = cols]

  by.xs <- list("full",
                c("pn", "sn", "ap", "am"),  ##next 3 are swaps
                c("pn", "sn_i", "ap", "am"),
                c("pn", "ap", "am"),
                c("pn","sn","ap"), ##next 4 names in wrong place
                c("pn","ap","am"),
                c("pn","sn"),
                c("pn","ap"))

  by.ys <- list("full",
                c("pn", "sn", "am", "ap"),
                c("pn", "sn_i", "am", "ap"),
                c("pn", "am", "ap"),
                c("pn","ap","am"),
                c("pn","sn","ap"),
                c("pn","ap"),
                c("pn","sn"))
  n <- length(by.xs)
  swap <- 1:n > 1
  lastname_swap <- c(1:n) %in% 2:4

  pms <- vector("list", n)
  
  message("Encontrando pareos exactos.")
  pb <-  txtProgressBar(1, n, style = 3)
  
  ## We use these to remove perfect matches and stop paring them
  keep_query <- rep(TRUE, nrow(query))
  keep_target <- rep(TRUE, nrow(target))
  
  for (i in 1:n) { # in for-loop because we change query and target
    setTxtProgressBar(pb, i)
    
    if (lastname_swap[i]) {
      ind <- as.character(query$ap) != as.character(query$am)
      ind[is.na(ind)] <- FALSE ##if last names same no point in swapping
      pms[[i]] <- perfect_match_engine(query[ind & keep_query], target[keep_target],
                                       by.x = by.xs[[i]], by.y = by.ys[[i]])
    } else{
      pms[[i]] <- perfect_match_engine(query[keep_query], target[keep_target],
                                       by.x = by.xs[[i]], by.y = by.ys[[i]])
    }
    
    is_full <- identical(by.xs[[i]], "full") & identical(by.ys[[i]],"full")
    if (!is.null(pms[[i]])) {
      if (nrow(pms[[i]]) > 0 & self.match) { ## remove repeated comparisons
        pms[[i]] <- pms[[i]][id.x != id.y]
        pms[[i]][, pair := fifelse(id.x < id.y, paste(id.x, id.y, sep = ":"), paste(id.y, id.x, sep = ":"))]
        pms[[i]] <- unique(pms[[i]], by = "pair")
        pms[[i]][, pair := NULL]
      }
      if (nrow(pms[[i]]) > 0) {
        pms[[i]]$swap <- swap[i]
        if (is_full & !self.match) {
          keep_query[query$id %in% unique(pms[[i]]$id.x)] <- FALSE
          keep_target[target$id %in% unique(pms[[i]]$id.y)] <- FALSE
          pms[[i]]$full_match <- TRUE
        } else{
          pms[[i]]$full_match <- FALSE
        }
      } else{
        pms[[i]] <- NULL
      }
    }
  }
  
  pms <- rbindlist(pms)
  
  if (!is.null(pms)) {
    if (nrow(pms) > 0) {
      pms$match <- factor("perfect", levels = c("perfect", "fuzzy"))
      pms$truncated <- FALSE
      cols <- c("pn", "sn", "ap", "am")
      for (cn in cols) {
        pms[[paste(cn, "freq", sep = "_")]] <- freq[[cn]]$freq[ match(pms[[ cn ]], freq[[cn]]$name) ]
      }
      pms$genero_match <- pms$genero.x == pms$genero.y
      pms$lugar_match <- pms$lugar.x == pms$lugar.y
      pms$dob_match <- pms$dob.x == pms$dob.y
      pms$truncated <- FALSE
    } else pms <- NULL
  }

  message("\nEncontrando pareos con errores.")

  fms <- fuzzy_match_engine(query[keep_query], target[keep_target],  
                            total.max = total.max,  full.max = full.max,
                            self.match = self.match, max.rows = max.rows, n.cores = n.cores)
  if(!is.null(fms)){
    if(nrow(fms)>0){
      fms[, total_dist := rowSums(.SD, na.rm=TRUE), .SDcols = patterns("(pn|sn|ap|am)_dist")]
      cols <- outer(c("full", "pn", "sn", "ap", "am"),
                    c(name = "", x = ".x", y = ".y", dist = "_dist", nchar = "_nchar", i_match = "_i_match"), paste0)


      for(i in 1:nrow(cols)){

        nchar.x <- nchar(fms[[ cols[i, "x"] ]])
        nchar.y <- nchar(fms[[ cols[i, "y"] ]])

        ## Because second last name (need to define truncate = "am") is often truncated, we check if one is a substring of the other
        ## if if check.truncated is true we make the distance 0
        fms$truncated <- FALSE
        if(check.truncated){
          if(cols[i,"name"] %in% truncate){
            ind <- which(( substr(fms[[ cols[i, "x"] ]], 1, nchar.y) == substr(fms[[ cols[i, "y"] ]], 1, nchar.y) |
                             substr(fms[[ cols[i, "x"] ]], 1, nchar.x) == substr(fms[[ cols[i, "y"] ]], 1, nchar.x)) & nchar.x != nchar.y)
            cn <- cols[i, "dist"]
            fms[ind, (cn) := 0]
            fms[ind, truncated := TRUE]
          }
        }

        fms[[ cols[i, "nchar"] ]] <-  pmax(nchar.x, nchar.y, na.rm = TRUE)
        if(cols[i] != "sn"){
          fms[[ cols[i, "i_match"] ]] <- substr(fms[[ cols[i, "x"]]], 1, 1) == substr(fms[[ cols[i, "y"]]], 1, 1)
        } else{
          fms$sn_i_match <- fms$sn_i.x == fms$sn_i.y
        }

        if(cols[i] != "full"){
          cn <- cols[i, "name"]
          freq.x <- freq[[cn]]$freq[ match(fms[[ cols[i, "x"] ]], freq[[cn]]$name) ]
          freq.y <- freq[[cn]]$freq[ match(fms[[ cols[i, "y"] ]], freq[[cn]]$name) ]

          fms[[ paste(cn, "freq", sep="_") ]] <- pmax(freq.x, freq.y) #if either is NA don't use, probably misspelled name
        }
      }

      fms$match <- factor("fuzzy", levels=c("perfect", "fuzzy"))
      fms$genero_match <- fms$genero.x == fms$genero.y
      fms$lugar_match <- fms$lugar.x == fms$lugar.y
      fms$dob_match <- fms$dob.x == fms$dob.y
      fms$full_match <- FALSE
    }
  }

  if(is.null(pms) & is.null(fms)) return(NULL)

  cols <- c("id.x", "id.y", 
            "full_dist", "pn_dist", "sn_dist", "ap_dist", "am_dist",
            "full_nchar", "pn_nchar", "sn_nchar", "ap_nchar", "am_nchar",
            "full_i_match",  "pn_i_match", "sn_i_match", "ap_i_match", "am_i_match",
            "pn_freq","sn_freq", "ap_freq", "am_freq",
            "dob_match", "genero_match", "lugar_match", "match", "full_match",
            "swap", "truncated")
  if(!is.null(pms)){
    if(nrow(pms) > 0){
      pms <- pms[,..cols]
      setnames(pms, "match", "match_type")
    } else pms <- NULL
  }
  if(!is.null(fms)){
    if(nrow(fms) > 0){
      fms <- fms[,..cols]
      setnames(fms, "match", "match_type")
    } else fms <- NULL
  }
  map <- rbindlist(list(pms, fms))
  return(map)
}

perfect_match_engine <- function(query, target, by=NULL, by.x=NULL, by.y=NULL){

  if(is.null(by.x)) by.x <- by
  if(is.null(by.y)) by.y <- by

  ## remove rows with NAs in the columns we are matching by
  ind.x <- !matrixStats::rowAnyNAs(as.matrix(query[, ..by.x]))
  ind.y <- !matrixStats::rowAnyNAs(as.matrix(target[, ..by.y]))

  ## if no rows left return NULL
  if(length(ind.x)==0L & length(ind.y)==0L) return(NULL)

  ## Now we merge
  map <- merge(query[ind.x], target[ind.y], by.x = by.x, by.y = by.y)
  ## this pattern later tells you what kind of match it was

  ## because this is perfect match all no NA columns have 0 distance for full
  ## and all matched if not full
  cols <- c(outer(c("full", "pn", "sn", "ap", "am"), c("dist", "nchar"), paste, sep = "_"))
  map[,(cols) := as.numeric(NA)]

  cols <- c(outer(c("full", "pn", "sn", "ap", "am"), c("i_match"), paste, sep = "_"))
  map[,(cols) := as.logical(NA)]

  if(identical(by.x, "full")){
    map[, full_dist := 0]
    map[, full_i_match := TRUE]
    map[, full_nchar := nchar(full)]
    for(cn in c("pn", "sn", "ap", "am")){
      ind <- !is.na(map[[paste(cn, "x", sep = ".")]]) & !is.na(map[[paste(cn, "y", sep = ".")]])
      map[[paste(cn, "dist", sep = "_")]] <- ifelse(ind, 0, NA)
      if(cn != "sn"){
        map[[paste(cn, "i_match", sep = "_") ]][ind] <- TRUE
      } else{
        map[[paste(cn, "i_match", sep = "_") ]][!is.na(map[["sn_i.x"]]) & !is.na(map[["sn_i.y"]])] <- TRUE
      }
      map[[paste(cn, "nchar", sep = "_")]] <-
        pmax(nchar(map[[ paste(cn, "x", sep = ".") ]]),
             nchar(map[[ paste(cn, "y", sep = ".") ]]),
             na.rm = TRUE)
    }
  } else{
    map[, full_dist := stringdist(full.x, full.y)]
    map[, full_nchar := pmax(nchar(full.x), nchar(full.y))]
    cols <- paste(by.x[by.x != "sn_i"], "dist", sep = "_")
    map[,(cols) := 0L]
    for (cn in by.x[!by.x %in% "sn_i"]) {
      map[[paste(cn, "nchar", sep = "_")]] <- nchar(map[[ cn ]])
    }
    cols <- paste(by.x[!by.x %in% "sn_i"], "i_match", sep = "_")
    map[,(cols) := TRUE]
  }

  ## remove and rename variables to keep only the original names
  ## we need this so that all the merged maps can be rbinded
  remove_cols <- intersect(paste0(c("full", "pn", "sn", "sn_i", "ap", "am"), ".y"), names(map))
  map <- map[,!..remove_cols]
  rename_cols <- intersect(paste0(c("full", "pn", "sn", "sn_i", "ap", "am"), ".x"), names(map))
  setnames(map, rename_cols, str_remove(rename_cols, ".x"))
  setcolorder(map, c("id.x",  "id.y", "full", "pn", "sn", "sn_i", "ap", "am", 
                     "dob.x", "dob.y",
                     "lugar.x", "lugar.y", "genero.x", "genero.y",
                     "full_dist", "pn_dist",  "sn_dist", "ap_dist", "am_dist",
                     "full_i_match", "pn_i_match",  "sn_i_match", "ap_i_match", "am_i_match",
                     "full_nchar", "pn_nchar",  "sn_nchar", "ap_nchar", "am_nchar"))
  return(map)
}

fuzzy_match_engine <- function(query, target, total.max = 8, full.max = 8, self.match = FALSE,
                               matches.max = 5, max.rows = 100, n.cores = NULL){

  qnames <- paste(names(query), "x", sep = ".")
  tnames <- paste(names(target), "y", sep = ".")
  
  tt <- copy(target)
  setnames(tt, tnames)
  
  if (is.null(n.cores)) n.cores <- pmax(detectCores() - 1, 1)  # Leave one core free
  n <- nrow(query)
  m <- pmax(ceiling(n/max.rows), n.cores)
  
  indexes <- split(1:n, cut(1:n, quantile(1:n, seq(0, 1, len = m + 1)), include.lowest = TRUE))

  cat("Calculando distancias. Dividiendo query en", m, "tabla(s)\n")
  cl <- snow::makeCluster(n.cores) 
  registerDoSNOW(cl) 
  pb <- txtProgressBar(max = m, style = 3)
  progress <- function(n) setTxtProgressBar(pb, n)
  fms <- foreach(ind = indexes, .combine = c, .options.snow = list(progress = progress)) %dopar% {
    
    if (self.match & length(ind) < 2) {
      return(NULL)
    } 
    
    qq <- query[ind,]
    setnames(qq, qnames)
    
    full_dist <- stringdistmatrix(qq$full.x, tt$full.y, method = "lv", nthread = 1)
    
    pn_dist <- stringdistmatrix(qq$pn.x, tt$pn.y, method = "lv", nthread = 1)
    
    if (self.match) { ##don't compare twice
      full_dist[lower.tri(full_dist, diag = TRUE)] <- Inf
      pn_dist[lower.tri(pn_dist, diag = TRUE)] <- Inf ## make pn_dist Inf instead of total so it stays Inf in the last name swap
    }
    
    pn_dist_nona <- pn_dist
    pn_dist_nona[is.na(pn_dist_nona)] <- 0
    
    sn_i_dist <- outer(qq$sn.x, tt$sn.y, `!=`)*1
    sn_i_dist[is.na(sn_i_dist)] <- 0
    
    sn_dist <- stringdistmatrix(qq$sn.x, tt$sn.y, method = "lv", nthread = 1)
    sn_dist_nona <- sn_dist
    sn_dist_nona[is.na(sn_dist_nona)] <- sn_i_dist[is.na(sn_dist_nona)]
    
    ## consider reversed last names
    ap_dist <- stringdistmatrix(qq$ap.x, tt$ap.y, method = "lv", nthread = 1)
    ap_dist_nona <- ap_dist
    ap_dist_nona[is.na(ap_dist_nona)] <- 0
    
    am_dist <- stringdistmatrix(qq$am.x, tt$am.y, method = "lv", nthread = 1)
    am_dist_nona <- am_dist
    am_dist_nona[is.na(am_dist_nona)] <- 0
    
    total <- pn_dist_nona + sn_dist_nona + ap_dist_nona + am_dist_nona
    
    ## for all the rows of the target that are within 6 errors of the query we keep
    matches <- lapply(1:nrow(total), function(j){
      ind <- which(total[j,] <= total.max | full_dist[j,] <= full.max)
      if (length(ind) == 0) return(NULL) else{
        if (length(ind) > matches.max) {
          total_rank <- rank(total[j,ind], ties.method = "min")
          full_rank <- rank(full_dist[j,ind], ties.method = "min")
          ind <- ind[pmin(total_rank, full_rank) <= matches.max]
        } else{ rank1 <- NA; rank2 <- NA}
        ret <- cbind(qq[j],
                     tt[ind],
                     data.table(full_dist = full_dist[j, ind], 
                                pn_dist = pn_dist[j,ind], 
                                sn_dist = sn_dist[j,ind], 
                                ap_dist = ap_dist[j,ind], 
                                am_dist =  am_dist[j,ind]))
        ret$swap <- FALSE
        return(ret)
      }
    })
    nomatch_ind <- sapply(matches, is.null)
    
    ## if no match look for swap last name match
    if (any(nomatch_ind)) {
      
      full_dist <- full_dist[nomatch_ind,,drop = FALSE]
      pn_dist <- pn_dist[nomatch_ind,,drop = FALSE]
      sn_dist <- sn_dist[nomatch_ind,,drop = FALSE]
      
      qq <- qq[nomatch_ind]
      
      ap_dist <- stringdistmatrix(qq$ap.x, tt$am.y, method = "lv", nthread = 1)
      
      am_dist <- stringdistmatrix(qq$am.x, tt$ap.y, method = "lv", nthread = 1)
      
      total <- pn_dist_nona[nomatch_ind,,drop = FALSE] +
        sn_dist_nona[nomatch_ind,,drop = FALSE] +
        ap_dist + am_dist
      
      matches2 <- lapply(1:nrow(total), function(j){
        ind <- which(total[j,] <= total.max)
        if (length(ind) == 0) return(NULL) else{
          if (length(ind) > matches.max) {
            total_rank <- rank(total[ind], ties.method = "min")
            rank1 <- full_rank[total_rank <= matches.max]
            rank2 <- total_rank[total_rank <= matches.max]
            ind <- ind[total_rank <= matches.max]
          } else{ total_rank <- NA; full_rank <- NA}
          ret <- cbind(qq[j],
                       tt[ind],
                       data.table(full_dist = full_dist[j, ind], 
                                  pn_dist = pn_dist[j,ind], 
                                  sn_dist = sn_dist[j,ind], 
                                  ap_dist = ap_dist[j,ind], 
                                  am_dist =  am_dist[j,ind],
                                  total_rank = rank2,
                                  full_rank = rank1))
          ret$swap <- TRUE
          return(ret)
        }
      })
    } else{
      matches2 <- NULL
    }
    c(matches, matches2)
  }
  close(pb)
  stopCluster(cl) 
  
  #cat("Combinando tablas.\n")
  fms <- rbindlist(fms)
  return(fms)
}

calibrate_matches <- function(map, mtry = 5){
  map <- copy(map)
  patterns <- list(c("pn", "sn", "ap", "am"),
                   c("pn", "sn_i", "ap", "am"),
                   c("pn", "ap", "am"),
                   c("pn", "sn", "ap"),
                   c("pn", "sn_i", "ap"),
                   c("pn", "ap"),
                   c("ap", "am"),
                   c("ap"),
                   c("pn", "sn"))
  
  message("\nCalibrando el pareo.")
  map[,nchar:=as.numeric(NA)]
  map[, prop_match := as.numeric(NA)]
  map[, score := as.numeric(NA)]
  map[, pattern := as.character(NA)]
  map[, full_prop_match := 1 - full_dist/full_nchar]
  map$pn_ap_match <- 1 - rowSums(map[,c("pn_dist", "ap_dist")], na.rm = TRUE)/
    rowSums(map[,c("pn_nchar", "ap_nchar")],na.rm = TRUE)
  ### form the prop match of the individual parts so we can pick best fit
  freq_cols <- paste(c("pn", "sn", "ap", "am"), "freq", sep="_")
  map[, (freq_cols) := lapply(.SD, function(x){
    x[is.na(x)] <- median(x, na.rm=TRUE)
    return(x)
  }), .SDcols = freq_cols]
  
  map[, sn_i_dist := as.numeric(!sn_i_match)]
  map[, sn_i_nchar := as.numeric(!is.na(sn_i_match))]
  
  ## check sn_i_match is NA for dc31beb8-54da-46a0-b54d-50ff6f5f608d
  ## compute prop_match for every pattern
  for(i in seq_along(patterns)){
    p <- patterns[[i]]
    dist_cols <- paste(p, "dist", sep = "_")
    nchar_cols <- paste(p, "nchar", sep="_")
    ind <- which(!matrixStats::rowAnyNAs(as.matrix(map[, ..dist_cols])) & is.na(map$prop_match))
    if(length(ind) > 0){
      map$prop_match[ind] <- 1 - rowSums(map[ind, ..dist_cols])/(rowSums(map[ind, ..nchar_cols]))
      map$nchar[ind] <- rowSums(map[ind,..nchar_cols])
      map$pattern[ind] <- paste(patterns[[i]], collapse = ":")
    }
  }
  new_i_match_cols <- paste(c("pn", "sn", "ap", "am"), "i_match_numeric", sep="_")
  i_match_cols <- paste(c("pn", "sn", "ap", "am"), "i_match", sep="_")
  map[, (new_i_match_cols) := lapply(.SD, function(x){
    x <- as.numeric(x)
    x[is.na(x)] <- 1
    return(x)
  }), .SDcols = i_match_cols]
  
  name_match_cols <- paste(c("pn", "sn", "ap", "am"), "match", sep = "_")
  map[, (name_match_cols) := lapply(.SD, function(x){x == 0 | is.na(x)}),
      .SDcols = paste(c("pn", "sn", "ap", "am"), "dist", sep = "_")]
  
  map$perfect <- as.numeric(map$match_type == "perfect")
  ### create training dataset of random forest
  setorder(map, id.x, -prop_match)
  train <- map[!is.na(lugar_match),.SD[1], by = id.x]
  train$y <- factor(train$lugar_match)
  cols <- c("y", "perfect", "prop_match", "nchar", "pattern", "swap", "truncated", new_i_match_cols, freq_cols, name_match_cols )
  train <- train[, ..cols]
  #library(caret)
  #library(doSNOW)
  #cl <- makeCluster(detectCores() - 1)
  ##registerDoSNOW(cl)
  ##control <- trainControl(method = "cv", number = 10, p = .9, , allowParallel = TRUE)
  ##fit <- train(y ~ ., method = "rf", data = train, tuneGrid = data.frame(mtry = 3:6), trControl = control)
  ##stopCluster(cl)
  cat("Ajustando modelo con random forest.\n")
  fit <- randomForest(y ~ ., data = train, mtry = mtry)
  cat("Predciendo probabilidades.\n")
  map$score <- predict(fit, newdata = map, type = "prob")[,2]
  map[, (name_match_cols) := NULL]
  map[, (new_i_match_cols) := NULL]
  map[, perfect := NULL]
  return(map)
}

# calibrate_matches <- function(map){
# 
#   map <- copy(map)
#   message("\nCalibrando el pareo.")
#   map[, prop_match := as.numeric(NA)]
#   map[, score := as.numeric(NA)]
#   map[, pattern := as.character(NA)]
#   map[, full_prop_match := 1 - full_dist/full_nchar]
#   map$pn_ap_match <- 1 - rowSums(map[,c("pn_dist", "ap_dist")], na.rm = TRUE)/
#     rowSums(map[,c("pn_nchar", "ap_nchar")],na.rm = TRUE)
#   ### form the prop match of the individual parts so we can pick best fit
#   total_dist <- rowSums(map[, c("pn_dist", "sn_dist", "ap_dist", "am_dist")], na.rm = TRUE)
#   total_nchar <- rowSums(sapply(c("pn", "sn", "ap", "am"), function(i){
#     x <- map[[paste(i,"dist", sep = "_")]]
#     y <- map[[paste(i,"nchar", sep = "_")]]
#     y[is.na(x)] <- 0
#     y
#   }))
#   sn_i_match <- map$sn_i_match 
#   sn_i_match[is.na(sn_i_match)] <- 1
#   total_dist <- total_dist + ifelse(is.na(map$sn_dist), 1 - sn_i_match*1, 0)
#   total_nchar <- total_nchar +  ifelse(is.na(map$sn_dist) & !is.na(map$sn_i_match), 1, 0)
#   map$total_prop_match <- 1 - total_dist/total_nchar
#   map[,max_match := pmax(total_prop_match, full_prop_match)]
#   ## order 
#   setorder(map, id.x, max_match)
#   map[,max_match := NULL]
#   
#   patterns <- list(c("pn", "sn", "ap", "am"),
#                    c("pn", "sn_i", "ap", "am"),
#                    c("pn", "ap", "am"),
#                    c("pn", "sn", "ap"),
#                    c("pn", "sn_i", "ap"),
#                    c("pn", "ap"),
#                    c("ap", "am"))
# 
#   ## what name frequencies to include in model
#   ## decided usign EDA
#   freqs <- list(c("ap","am"),
#                 c("ap", "am"),
#                 c("pn", "ap", "am"),
#                 c("ap"),
#                 c("pn", "ap"),
#                 c("pn", "ap"),
#                 c("ap","am"))
# 
#   ## for names with missing frequencies impute the median
#   freq_impute <- function(x){
#     x[is.na(x)] <- median(x, na.rm=TRUE)
#     return(x)
#   }
#   cols <- paste(c("pn", "sn", "ap", "am"), "freq", sep="_")
#   map[, (cols) := lapply(.SD, freq_impute), .SDcols = cols]
#  
#   map[, sn_i_dist := as.numeric(!sn_i_match)]
#   map[, sn_i_nchar := as.numeric(!is.na(sn_i_match))]
#   
#   for(i in seq_along(patterns)){
#     p <- patterns[[i]]
#     print(p)
#     dist_cols <- paste(p, "dist", sep="_")
#     nchar_cols <- paste(p, "nchar", sep="_")
#     if(!is.null(freqs[[i]])){
#       freq_cols <- paste(freqs[[i]], "freq", sep="_")
#     } else freq_cols <- NULL
# 
#     ind <- !matrixStats::rowAnyNAs(as.matrix(map[, ..dist_cols])) &
#       is.na(map[,score]) ## needed columns not NA and not yet scored
# 
#     map$prop_match[ind] <- 1 - rowSums(map[ind,..dist_cols])/(rowSums(map[ind,..nchar_cols]))
#   
#     the_formula <- paste("dob_match ~ pmax(prop_match,0.6)")
#     if(!is.null(freq_cols)){
#       the_formula <- paste(the_formula, "+",
#                            paste0("log(pmax(", freq_cols, ",10^-4))", collapse = " + "))
#     }
#     if(sum(map[ind & !is.na(dob_match)]$swap) >= 100) the_formula <- paste(the_formula, "swap", sep="+")
#     the_formula <- formula(the_formula)
#     
#     ## now pick best fit so we don't fit glm to all the garbage
#     dat <- map[ind & !is.na(dob_match), .SD[1], by = id.x]
#     print(nrow(dat))
#     fit <- try(glm(the_formula, family = "binomial", data = dat), silent=TRUE)
#     if(class(fit)[1]=="try-error" | fit$converged == ""){
#       warning(paste0("Not enough data to fit model for pattern ",  paste(p, collapse = ":"),
#                      ". Returing NA"))
#       map[ind, score := NA]
#     } else{
#       map[ind, score := predict(fit, newdata = map[ind], type = "response")]
#     }
#     print(p)
#     map[ind, pattern := paste(p, collapse = ":")]
#   }
# 
#   map[, pattern := factor(pattern, levels = sapply(patterns, paste, collapse = ":"))]
#   map[, score := (score - min(score, na.rm = TRUE)) / max(score - min(score,na.rm = TRUE), na.rm = TRUE)]
# 
#   return(map)
# }


cleanup_matches <- function(map, query, target, self.match, cutoff = 0){

  message("\nLimpiando pareos.")
  ## map must be output of calibrate map
  map <- copy(map)
  map <- map[!is.na(score)]

  ## Pick the best score for each id.x
  if(!self.match){
    map[order(id.y, !dob_match, !genero_match, !lugar_match, pattern, full_prop_match)]
    map <- map[map[,.I[which.max(score)], by = id.x]$V1]
  } else{
    map[order(id.y, !dob_match, !genero_match, !lugar_match, pattern, full_prop_match)]
    map <- map[map[,.I[which.max(score)], by = id.y]$V1]
    map <- unique(map, by=c("id.x", "id.y"))
  }

  map <- merge(merge(map, query, by.x = "id.x", by.y = "id", all.x = TRUE), 
               target, by.x = "id.y", by.y = "id", all.x = TRUE)
  map$dob_dist <- stringdist(map$dob.x, map$dob.y)
  cols <- c("id.x", "id.y", "original.x", "original.y", "dob.x", "dob.y", "score", "dob_dist",
            "prop_match", "full_prop_match", "pn_ap_match", "lugar_match", 
            "genero_match", "pattern", "match_type", "full_match", "swap", "truncated")
  return(map[score >= cutoff, ..cols][order(score, decreasing = TRUE)])
}

