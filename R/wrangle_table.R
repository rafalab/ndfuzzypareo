#' Clean up names in preparation for match
#'
#' @param tab a data.table with the names
#'
#' @export
#' @import data.table
#' @import stringr
#' @importFrom forcats fct_na_value_to_level

wrangle_table <- function(tab){

  tab <- copy(tab)
  message("Arrenglando nombres con artículos.")
  dela_strings <- c("y", "de", "el", "la", "las", "los", "del", "lo", "di",
                    "da", "le","st", "mc", "mac", "van", "san", "dos", "el", "d", "o",
                    "de la", "de las", "de los","de lo")

  tab[, original := paste(as.character(fct_na_value_to_level(pn, "")),
                          as.character(fct_na_value_to_level(sn, "")),
                          as.character(fct_na_value_to_level(ap, "")),
                          as.character(fct_na_value_to_level(am, "")), sep="|")]
  cols <-   cols <- c("pn", "sn", "ap", "am", "genero", "lugar")
  tab[, (cols) := lapply(.SD, forcats::fct_recode, NULL = ""), .SDcols = cols]
  ## covert to factor to join de la x into delax
  cols <- c("pn", "sn", "ap", "am")
  tab[, (cols) :=  lapply(.SD, fct_trim), .SDcols = cols]
  tab[, (cols) :=  lapply(.SD, fct_to_lower), .SDcols = cols]
  tab[, (cols) :=  lapply(.SD, fix_names), .SDcols = cols]
  dela_index <- as.character(tab$ap) %in% dela_strings
  if(any(dela_index)){
    cols2 <- c("ap", "am")
    tab[, (cols2) := lapply(.SD, as.character), .SDcols = cols2]
    tab[dela_index & !is.na(am), `:=`(ap = paste0(ap, am), am = as.character(NA))]
    tab[, (cols2) := lapply(.SD, factor), .SDcols = cols2]
  }
  tab[, (cols) := lapply(.SD, join_dela), .SDcols = cols]

  ## covert ro chacters for split
  message("Creando cadena con nombre completo.")

  tab[, full := str_remove_all(paste0(as.character(fct_na_value_to_level(pn, "")),
                                      as.character(fct_na_value_to_level(sn, "")),
                                      as.character(fct_na_value_to_level(ap, "")),
                                      as.character(fct_na_value_to_level(am, ""))), "\\s+")]

  tab[, (cols) := lapply(.SD, as.character), .SDcols = cols]
  ## move second last name if two names in one
  message("Encontrando segundos nombres/apellidos en columna incorrecta.")
  split_pn <- str_split_fixed(tab$pn, "\\s+", n = 3) ## 3 to catch the extra, do not merge
  split_ap <- str_split_fixed(tab$ap, "\\s+", n = 3)
  ## IF the middle initial was moved to the last name field:
  ind <- nchar(split_ap[,1]) == 1 & split_ap[,2] != "" & split_pn[,2] == "" & is.na(tab$sn)
  tab[ , `:=`(sn = fifelse(ind, split_ap[,1], sn),
              ap = fifelse(ind, split_ap[,2], ap),
              am = fifelse(ind & is.na(am) & split_ap[,3]!="", split_ap[,3], am))]

  split_ap <- str_split_fixed(tab$ap, "\\s+", n = 3)
  ## get an alternative second name from the first name
  tab[, `:=`(pn = fifelse(split_pn[,1] == "", as.character(NA), split_pn[,1]),
             alt_sn = fifelse(split_pn[,2] == "", as.character(NA), split_pn[,2]))]
  ## get an alternative maternal last name from paternal last name
  tab[, `:=`(ap = fifelse(split_ap[,1] == "", as.character(NA), split_ap[,1]),
             alt_am = fifelse(split_ap[,2] == "", as.character(NA), split_ap[,2]))]
  tab[, sn := fifelse(is.na(sn), alt_sn, sn)]
  tab[, am := fifelse(is.na(am), alt_am, am)]
  ## get middle initial
  ## get rid of extra names
  split_sn <- str_split_fixed(tab$sn, "\\s+", n = 2) ## 3 to catch the extra, do not merge
  split_am <- str_split_fixed(tab$am, "\\s+", n = 2)
  tab[, sn := fifelse(is.na(sn), as.character(NA), split_sn[,1])]
  tab[, am := fifelse(is.na(am), as.character(NA), split_am[,1])]
  tab[, sn_i := factor(str_sub(tab$sn, 1, 1))]


  tab[, alt_sn := NULL]
  tab[, alt_am := NULL]
  tab[, (cols) := lapply(.SD, factor), .SDcols = cols]
  ## now fix names
  tab[, (cols) := lapply(.SD, join_dela), .SDcols = cols]
  tab[, (cols) := lapply(.SD, fix_names), .SDcols = cols]
  tab[fct_nchar(sn)< 2, sn := as.factor(NA)]
  tab[is.na(ap)  & !is.na(am),`:=`(ap = am, am = as.factor(NA))]
  message("Cambiando abreviaciones.")
  tab[ap == "rodz|rdz", ap := "rodriguez"]
  tab[am %in% c("rodz","rdz"), am := "rodriguez"]
  tab[ap %in% c("hdez", "hndz"), ap := "hernandez"]
  tab[am %in% c("hdez", "hndz"), am := "hernandez"]
  tab[ap %in% c("fndz", "fdez"), ap := "fernandez"]
  tab[am %in% c("fndz", "fdez"), am := "fernandez"]
  tab[ap == "mndz", ap := "mendez"]
  tab[am == "mndz", am := "mendez"]
  tab[ap %in% c("mrtz", "mtz"), ap := "martinez"]
  tab[am %in% c("mrtz", "mtz"), am := "martinez"]
  tab[ap == "glez", ap := "gonzalez"]
  tab[am == "glez", am := "gonzalez"]
  tab[ap == "gmez", ap := "gomez"]
  tab[am == "gmez", am := "gomez"]
  tab[ap == "stgo", ap := "santiago"]
  tab[am == "stgo", am := "santiago"]
  tab[ap == "ga", ap := "garcia"]
  tab[am == "ga", am := "garcia"]
  tab[pn == "fco", pn := "francisco"]
  tab[sn == "fco", sn := "francisco"]
  tab[ap == "fco", ap := "francisco"]
  tab[am == "fco", am := "francisco"]
  tab[, (cols) := lapply(.SD, droplevels), .SDcols = cols]

  setcolorder(tab, c("id", "original", "full",
                     "pn", "sn", "sn_i", "ap", "am",
                     "genero", "lugar", "dob"))
  ##has to have last_name
  tab <- tab[!is.na(ap)]

  return(tab)
}


