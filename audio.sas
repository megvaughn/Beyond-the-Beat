cas mySession terminate;
cas mySession;
libname casuser cas caslib="casuser";


/* load artists */
proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/artists_clean.csv"
      outcaslib="casuser"
      casout="spotify_artists_clean"
      replace;
quit;


/* load song chunks 1-10 */
proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_1.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_2.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_3.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_4.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_5.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_6.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_7.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_8.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_9.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


proc casutil;
 load file="/export/viya/homes/mcv20m@fsu.edu/casuser/chunk_10.csv"
      outcaslib="casuser"
      casout="spotify_songs_chunk_1"
      replace;
quit;


/* load 10 song chunks into cas macro loop */
%let home=/export/viya/homes/mcv20m@fsu.edu/casuser;


%macro load_chunks(n=10);
 %do i=1 %to &n;
   proc casutil;
     load file="&home/chunk_&i..csv"
          outcaslib="casuser"
          casout="spotify_songs_chunk_&i"
          replace;
   quit;
 %end;
%mend;


%load_chunks(n=10);


/* drop old combined table if it exists */
proc casutil;
 droptable incaslib="casuser" casdata="spotify_songs_clean" quiet;
quit;


/* combine all chunks into one cas table */
proc fedsql sessref=mySession;
 create table casuser.spotify_songs_clean as
 select * from casuser.spotify_songs_chunk_1
 union all select * from casuser.spotify_songs_chunk_2
 union all select * from casuser.spotify_songs_chunk_3
 union all select * from casuser.spotify_songs_chunk_4
 union all select * from casuser.spotify_songs_chunk_5
 union all select * from casuser.spotify_songs_chunk_6
 union all select * from casuser.spotify_songs_chunk_7
 union all select * from casuser.spotify_songs_chunk_8
 union all select * from casuser.spotify_songs_chunk_9
 union all select * from casuser.spotify_songs_chunk_10
 ;
quit;


/* validation checks */
proc cas;
 table.tableinfo / caslib="casuser";
quit;


/* row count */
proc fedsql sessref=mySession;
 select count(*) as n_rows from casuser.spotify_songs_clean;
quit;


/* column names */
proc cas;
 table.columninfo / table={name="spotify_songs_clean", caslib="casuser"};
quit;


/* spot check duplicates by track id */
proc fedsql sessref=mySession;
 select count(*) as n_rows
 from casuser.spotify_songs_clean;
quit;


/* genre distrubution table */
title "Genre Distribution (Counts)";
proc fedsql sessref=mySession;
 select genre, count(*) as n
 from casuser.spotify_songs_clean
 group by genre
 order by n desc;
quit;
title;


/* genre distribution (bar chart), (top 20) */
proc fedsql sessref=mySession;
 create table casuser.genre_counts as
 select genre, count(*) as n
 from casuser.spotify_songs_clean
 group by genre;
quit;


title "Top 20 Genres by Count";
proc sgplot data=casuser.genre_counts(obs=20);
 vbar genre / response=n datalabel;
 xaxis display=(nolabel) fitpolicy=rotate;
 yaxis grid label="Number of Songs";
run;
title;


/* summary stats (table) */
title "Audio Feature Summary Statistics";
proc means data=casuser.spotify_songs_clean n mean std min p25 median p75 max;
 var danceability energy valence speechiness acousticness instrumentalness
     liveness loudness tempo duration_ms;
run;
title;


/* distributions (histograms) */
title "Distribution of Danceability";
proc sgplot data=casuser.spotify_songs_clean;
 histogram danceability;
 density danceability;
run;
title;


title "Distribution of Energy";
proc sgplot data=casuser.spotify_songs_clean;
 histogram energy;
 density energy;
run;
title;


title "Distribution of Tempo";
proc sgplot data=casuser.spotify_songs_clean;
 histogram tempo;
 density tempo;
run;
title;




/* scatter plot */
title "Energy vs. Danceability";
proc sgplot data=casuser.spotify_songs_clean;
 scatter x=danceability y=energy / transparency=0.7;
 xaxis grid;
 yaxis grid;
run;
title;


/* correlation table */
title "Correlation Matrix (Audio Features)";
proc corr data=casuser.spotify_songs_clean nosimple;
 var danceability energy valence speechiness acousticness instrumentalness
     liveness loudness tempo duration_ms;
run;
title;


/* build modeling base table */
/* drop + rebuild modeling base */
proc casutil; droptable incaslib="casuser" casdata="spotify_model_base" quiet; quit;


proc fedsql sessref=mySession;
 create table casuser.spotify_model_base as
 select
   genre,
   /* audio features (edit if needed) */
   danceability, energy, valence, speechiness, acousticness,
   instrumentalness, liveness, loudness, tempo, duration_ms
   /* lyrics column if you have it (optional for now) */
   /* , lyrics */
 from casuser.spotify_songs_clean
 where genre is not null
   and danceability is not null
   and energy is not null
   and tempo is not null
 ;
quit;


/* stratified split (70/30) using cas */
proc casutil; droptable incaslib="casuser" casdata="spotify_split" quiet; quit;


proc cas;
 sampling.stratified /
   table   ={name="spotify_model_base", caslib="casuser"}
   target  ="genre"
   samppct =70
   seed    =42
   partInd =true
   output  ={casout={name="spotify_split", caslib="casuser", replace=true}, copyvars="ALL"};
quit;


/* see the partition column name */
proc cas;
 table.columninfo / table={name="spotify_split", caslib="casuser"};
quit;


/* audio only model, random forest */
/* train */
proc cas;
 decisionTree.forestTrain /
   table    ={name="spotify_split", caslib="casuser", where="_PartInd1_=1"}
   target   ="genre"
   inputs   ={"danceability","energy","valence","speechiness","acousticness",
              "instrumentalness","liveness","loudness","tempo","duration_ms"}
   nominals ={"genre"}
   nTree    =200
   maxLevel =20     /* use maxLevel (NOT maxDepth) */
   seed     =42
   casOut   ={name="rf_audio_model", caslib="casuser", replace=true};
quit;


/* score test */
proc cas;
 decisionTree.forestScore /
   table   ={name="spotify_split", caslib="casuser", where="_PartInd1_=0"}
   model   ={name="rf_audio_model", caslib="casuser"}
   copyVars={"genre"}
   casOut  ={name="rf_audio_scored", caslib="casuser", replace=true};
quit;


/* find predicted class column name */
proc cas;
 table.columninfo / table={name="rf_audio_scored", caslib="casuser"};
quit;


/* audio only confusion matrix */
data work.rf_audio_scored_w;
 set casuser.rf_audio_scored;
run;


title "Confusion Matrix (Audio-only)";
proc freq data=work.rf_audio_scored_w;
 tables genre*_RF_PredName_ / nocol norow nopercent;
run;
title;


/* confusion matrix (audio) showing per-genre performance */
title "Confusion Matrix (Audio-only) Per-Genre Performance";
proc freq data=work.rf_audio_scored_w;
 tables genre*_RF_PredName_ / norow nocol;
run;
title;


/* build metrics table for audio only model */
/* 0) Build confusion-matrix counts as a dataset */
proc freq data=work.rf_audio_scored_w noprint;
 tables genre*_RF_PredName_ / out=work.cm_counts(drop=percent);
run;


/* 1) Totals by actual class (row totals) */
proc sql;
 create table work.actual_tot as
 select genre as class length=32,
        sum(count) as actual_total
 from work.cm_counts
 group by genre;
quit;


/* 2) Totals by predicted class (column totals) */
proc sql;
 create table work.pred_tot as
 select _RF_PredName_ as class length=32,
        sum(count) as pred_total
 from work.cm_counts
 group by _RF_PredName_;
quit;


/* 3) True positives by class (diagonal) */
proc sql;
 create table work.tp as
 select genre as class length=32,
        sum(count) as TP
 from work.cm_counts
 where genre = _RF_PredName_
 group by genre;
quit;


/* 4) Combine + compute per-class precision/recall/f1 */
proc sql;
 create table work.per_class_metrics as
 select
   a.class,
   coalesce(t.TP, 0) as TP,
   a.actual_total,
   p.pred_total,
   case when p.pred_total > 0 then (coalesce(t.TP,0) / p.pred_total) else . end as precision,
   case when a.actual_total > 0 then (coalesce(t.TP,0) / a.actual_total) else . end as recall,
   case
     when calculated precision is not missing and calculated recall is not missing
          and (calculated precision + calculated recall) > 0
     then 2 * calculated precision * calculated recall / (calculated precision + calculated recall)
     else .
   end as f1
 from work.actual_tot a
 left join work.pred_tot p on a.class = p.class
 left join work.tp t       on a.class = t.class;
quit;


/* 5) Macro averages + overall accuracy in one final table */
proc sql;
 create table work.audio_model_metrics as
 select
   /* overall accuracy = sum(diagonal) / N */
   (select sum(TP) from work.per_class_metrics) /
   (select sum(actual_total) from work.per_class_metrics) as Accuracy,


   /* macro averages = mean over classes */
   avg(precision) as Precision,
   avg(recall)    as Recall,
   avg(f1)        as F1
 from work.per_class_metrics;
quit;


title "Audio-only Model Metrics";
proc print data=work.audio_model_metrics noobs;
run;
title;


/* build lyrics mdoel */
/* Make sure lyrics exists + isn’t missing */
proc fedsql sessref=mySession;
 create table casuser.spotify_lyrics_base as
 select
   genre,
   lyrics
 from casuser.spotify_songs_clean
 where genre is not null
   and lyrics is not null
   and length(trim(lyrics)) > 0
 ;
quit;


/* Stratified split (70/30) */
proc casutil;
 droptable incaslib="casuser" casdata="spotify_lyrics_split" quiet;
quit;


proc cas;
 sampling.stratified /
   table   ={name="spotify_lyrics_base", caslib="casuser"}
   target  ="genre"
   samppct =70
   seed    =42
   partInd =true
   output  ={casout={name="spotify_lyrics_split", caslib="casuser", replace=true}, copyvars="ALL"};
quit;


/* confirm partition column */
proc cas;
 table.columninfo / table={name="spotify_lyrics_split", caslib="casuser"};
quit;

/* correct v. incorrect classifications */

/* 1) Add correctness flag */
data casuser.rf_audio_scored_flag;
  set casuser.rf_audio_scored;
  length correct $9;
  if strip(genre) = strip(_RF_PredName_) then correct="Correct";
  else correct="Incorrect";
run;

/* 2) Count correct/incorrect per actual genre */
proc fedsql sessref=mySession;
  create table casuser.rf_correct_counts as
  select
    genre,
    correct,
    count(*) as n
  from casuser.rf_audio_scored_flag
  group by genre, correct;
quit;

/* (optional) Pull to WORK for plotting (often more reliable) */
data work.rf_correct_counts;
  set casuser.rf_correct_counts;
run;

/* 3) Plot: clustered bars by genre */
title "Audio-only Model: Correct vs Incorrect Predictions by Genre (Test Set)";
proc sgplot data=work.rf_correct_counts;
  vbar genre / response=n group=correct groupdisplay=cluster datalabel;
  xaxis display=(nolabel) fitpolicy=rotate;
  yaxis grid label="Number of Songs";
run;
title;