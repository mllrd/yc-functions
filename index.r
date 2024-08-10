#################### data table cloud functions ########################
library(data.table)
library(httr)
library(jsonlite)
library(rvest)
library(stringr)
library(lubridate)

path_info <- "/function/storage/bucket/"
path_tournament <- "/function/storage/ruchess/"

get_perfomance <- function(name, type) {
   url <- paste0('https://lichess.org/api/user/',name,'/perf/',type)
   perfomance <- GET(url)
   if (status_code(perfomance)==200) {
      return(content(perfomance))
   }
   else {
      print("Status not 200")
      print(status_code(perfomance))
   }
}

get_activity <- function(name) {
   url <- paste0('https://lichess.org/api/user/',name,'/activity')
   result_query <- GET(url)
   if (status_code(result_query)==200) {
      return(content(result_query))
   }
   else {
      print("Status not 200")
      print(status_code(result_query))
   }
}

get_tournaments <- function(x) {
   pages <- read_html(paste0("https://ratings.ruchess.ru/tournaments?page=",x))
   table <- html_table(pages)[[1]] |> setDT()
   setnames(table,
            names(table),
            c("name","control","region","city","date_start","date_end","fed"))
   trnmts <- html_node(pages, "table")  |>
      html_elements("tr") |>
      html_element("a") |>
      html_attr("href") |>
      str_extract(pattern = "/tournaments/\\d*") |>
      na.omit()
   cols_dates <- c("date_start", "date_end")
   
   return(table[,
                "tournaments" := trnmts
   ][,
     (cols_dates):= lapply(.SD,as.Date,"%d.%m.%Y"),
     .SDcols = cols_dates
   ][,
     tournaments:=str_remove(tournaments,
                             pattern="/tournaments/")
   ][,
     tournaments:=as.integer(tournaments)
   ])
}

update_dataset <- function(name){
   df_one <- get_activity(name)
   if (is.list(df_one)){
      write_json(df_one,
                 paste0(path_info,name,
                        '_activity_',
                        format(Sys.time(),"%Y-%m-%d",tz="GMT"),
                        '.json'))
      if (!is.null(df_one[[1]]$puzzles)) {
         dtm <- as_datetime(df_one[[1]]$interval$start/1000) |> as.Date()
         dt_act <-c(df_one[[1]]$puzzles$score[c("win","loss","draw")] |> as.integer(),
                    df_one[[1]]$puzzles$score$rp$after |> as.integer()
         ) |> unlist() |> unname()
      } else {
         dtm <- as_datetime(`df_one`[[1]]$interval$start/1000) |> as.Date()
         dt_act <- c(rep(NA,4))
      }
      if (name==Sys.getenv("PLAYER_TWO")) {
         if (!is.null(df_one[[1]]$games$bullet)) {
            dt_bullet <- c(df_one[[1]]$games$bullet[c("win","loss","draw")],
                           df_one[[1]]$games$bullet$rp$after) |>
               unlist() |>
               unname() |>
               as.integer()
         } else {
            dt_bullet <- c(rep(NA, 4))
         }
         gg <- get_perfomance(name,"bullet")
         write_json(gg,
                    paste0(path_info,
                       name,"_bullet_",
                       '_perfomance_',
                       format(Sys.time(),"%Y-%m-%d",tz="GMT"),
                       '.json'))
         stat_count <- c(gg$stat$count[c("win","loss","draw","rated","opAvg")],
                         gg$perf$glicko$deviation)  |> unlist() |> unname()
      } else {
         if (!is.null(df_one[[1]]$games$blitz)) {
            dt_bullet <- c(df_one[[1]]$games$blitz[c("win","loss","draw")],
                           df_one[[1]]$games$blitz$rp$after) |>
               unlist() |>
               unname() |>
               as.integer()
         } else {
            dt_bullet <- c(rep(NA, 4))
         }
         gg <- get_perfomance(name,"blitz")
         write_json(gg,
                    paste0(path_info, name,"_blitz_",
                           '_perfomance_',
                           format(Sys.time(),"%Y-%m-%d",tz="GMT"),
                           '.json'))
         stat_count <- c(gg$stat$count[c("win","loss","draw","rated","opAvg")],
                         gg$perf$glicko$deviation)  |> unlist() |> unname()
      }
   }
   df <- as.data.frame(x=list(dtm=dtm,
                              win_puzzles=dt_act[1],
                              loss_puzzles=dt_act[2],
                              draw_puzzles=dt_act[3],
                              rp_puzzles=dt_act[4],
                              win_game=dt_bullet[1],
                              loss_game=dt_bullet[2],
                              draw_game=dt_bullet[3],
                              rp_game=dt_bullet[4],
                              win=stat_count[1],
                              loss=stat_count[2],
                              draw=stat_count[3],
                              rated=stat_count[4],
                              opAvg=stat_count[5],
                              deviation=stat_count[6],
                              name=name))
   return(df)
}

handler <- function(event, context) {
   
   fwrite(update_dataset(Sys.getenv("PLAYER_ONE")),
          file = paste0(path_info,"dataset.csv"),
          append = TRUE)
   
   fwrite(update_dataset(Sys.getenv("PLAYER_TWO")),
          file = paste0(path_info,"dataset.csv"),
          append = TRUE)
   
   get_tournaments(1) |>
      rbind(get_tournaments(2)) |>
      fwrite(paste0(path_tournament,
                    "tournaments_",
                    format(Sys.time(),
                           "%Y-%m-%d",
                           tz="GMT"),
                    ".csv.gz"))
   return(list(statusCode = 200, body = "Dataset updated; files saved!"))
}
