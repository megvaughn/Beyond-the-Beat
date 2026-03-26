/* import datasets */
cas mySession terminate;
cas mySession;
libname casuser cas caslib="casuser" datalimit=2G;


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

/* audio model */
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

/* lyric model */
/* lyric only model */
proc cas;
  table.columninfo / table={name="spotify_songs_clean", caslib="casuser"};
quit;

/* 1) Build lyrics-only base table */
proc casutil;
  droptable incaslib="casuser" casdata="spotify_lyrics_base" quiet;
  droptable incaslib="casuser" casdata="spotify_lyrics_split" quiet;
quit;

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

/* 2) Stratified split 70/30 */
proc cas;
  sampling.stratified /
    table   ={name="spotify_lyrics_base", caslib="casuser"}
    target  ="genre"
    samppct =70
    seed    =42
    partInd =true
    output  ={casout={name="spotify_lyrics_split", caslib="casuser", replace=true}, copyvars="ALL"};
quit;

proc fedsql sessref=mySession;
  select _PartInd1_ as part, count(*) as n
  from casuser.spotify_lyrics_split
  group by _PartInd1_;
quit;

/* make cas table */
proc casutil;
  droptable incaslib="casuser" casdata="spotify_lyrics_small" quiet;
quit;

proc cas;
  sampling.stratified /
    table   ={name="spotify_lyrics_base", caslib="casuser"}
    target  ="genre"
    samppct =5
    seed    =42
    output  ={casout={name="spotify_lyrics_small", caslib="casuser", replace=true}, copyvars="ALL"};
quit;

/* pull split table to WORK and clean lyrics */
data work.lyrics_w;
  set casuser.spotify_lyrics_split(keep=genre lyrics _PartInd1_);
  length lyrics_clean $32767;
  lyrics_clean = lowcase(compbl(
    prxchange('s/[^a-z0-9 ]+/ /', -1, coalescec(lyrics,''))
  ));
run;

/* build hashed features */
%let K=500;

proc ds2 sessref=mySession;
  data casuser.lyrics_hash_cas (overwrite=yes);

    dcl varchar(64) genre;
    dcl varchar(32767) lyrics;
    dcl int _PartInd1_;

    dcl varchar(32767) lyrics_clean;
    dcl varchar(50) word;
    dcl varchar(32) hx;
    dcl int t h i;

    /* --- declare feature columns explicitly --- */
    %macro decl_feats(K);
      %do j=1 %to &K;
        dcl double feat&j;
      %end;
    %mend;
    %decl_feats(&K);

    /* --- array over the declared columns --- */
    dcl double x[&K];

    method run();
      set casuser.spotify_lyrics_split;

      /* point array elements to the variables */
      %macro bind_feats(K);
        %do j=1 %to &K;
          x[&j] = feat&j;
        %end;
      %mend;

      /* clean */
      lyrics_clean = lowcase(prxchange('s/[^a-z0-9 ]+/ /', -1, coalesce(lyrics,'')));

      /* init */
      do i=1 to &K;
        x[i] = 0;
      end;

      /* hash tokens */
      t = 1;
      do while(scan(lyrics_clean, t, ' ') ne '');
        word = scan(lyrics_clean, t, ' ');
        if length(word) >= 3 then do;
          hx = put(md5(word), $hex32.);
          h  = mod(inputn(substr(hx,1,8), 'HEX8.'), &K) + 1;
          x[h] = x[h] + 1;
        end;
        t = t + 1;
      end;

      /* copy array back into feat1-feat&K */
      %macro write_feats(K);
        %do j=1 %to &K;
          feat&j = x[&j];
        %end;
      %mend;
      %write_feats(&K);

      output;
    end;

  enddata;
run;
quit;

/* check */
proc fedsql sessref=mySession;
  select count(*) as n from casuser.lyrics_hash_cas;
quit;

/* train model in cas */
proc gradboost data=casuser.lyrics_hash_cas;
  target genre / level=nominal;
  input feat1-feat&K / level=interval;

  /* adjust validate value if yours is 2 instead of 0 */
  partition rolevar=_PartInd1_(train='1' validate='0');
run;

/* check partition */
proc contents data=casuser.LYRICS_HASH_CAS; run;

proc fedsql sessref=mySession;
  select genre, count(*)
  from casuser.LYRICS_HASH_CAS
  group by genre
  order by count(*) desc;
quit;



proc casutil;
  list tables incaslib="casuser";
quit;

/* train and score */
proc gradboost data=casuser.lyrics_hash_cas;
  target genre / level=nominal;
  input feat1-feat&K / level=interval;

  /* use your true split values here */
  partition rolevar=_PartInd1_(train='1' validate='0');

  score out=casuser.lyrics_scored copyvars=(genre _PartInd1_);
run;

/* confirm train */
proc fedsql sessref=mySession;
  select _PartInd1_, count(*) as n
  from casuser.lyrics_hash_cas
  group by _PartInd1_;
quit;

proc contents data=casuser.lyrics_scored; run;


/* confirm scored data, pull sample */
proc casutil;
  list tables incaslib="casuser";
quit;

data work.lyrics_scored_samp;
  set casuser.lyrics_scored(obs=2000);
run;

proc contents data=work.lyrics_scored_samp; run;

/* single predicted label */
data casuser.lyrics_pred;
  set casuser.lyrics_scored;

  length pred_genre $64;
  maxp = P_genreBlues;
  pred_genre = "Blues";

  if P_genreClassical  > maxp then do; maxp=P_genreClassical;  pred_genre="Classical";  end;
  if P_genreCountry    > maxp then do; maxp=P_genreCountry;    pred_genre="Country";    end;
  if P_genreElectronic > maxp then do; maxp=P_genreElectronic; pred_genre="Electronic"; end;
  if P_genreFolk       > maxp then do; maxp=P_genreFolk;       pred_genre="Folk";       end;
  if P_genreHip_Hop    > maxp then do; maxp=P_genreHip_Hop;    pred_genre="Hip-Hop";    end;
  if P_genreJazz       > maxp then do; maxp=P_genreJazz;       pred_genre="Jazz";       end;
  if P_genrePop        > maxp then do; maxp=P_genrePop;        pred_genre="Pop";        end;
  if P_genreR_B        > maxp then do; maxp=P_genreR_B;        pred_genre=cats("R","&","B"); end;
  if P_genreRock       > maxp then do; maxp=P_genreRock;       pred_genre="Rock";       end;

  drop maxp;
run;


proc fedsql sessref=mySession;
  select 
    count(distinct I_genre) as n_actual,
    count(distinct pred_genre) as n_pred
  from casuser.lyrics_pred;
quit;

/* accuracy */
data casuser.test_scored;
  set casuser.lyrics_scored;
  where _PartInd1_ = 0;   /* <-- change if your test code is different */
  correct = (strip(genre) = strip(I_genre));
run;

proc means data=casuser.test_scored noprint;
  var correct;
  output out=casuser.acc mean=accuracy;
run;

/* 1) confusion matrix (actual x predicted) */
proc fedsql sessref=mySession;
  create table casuser.cm as
  select
    genre   as actual,
    I_genre as predicted,
    count(*) as n
  from casuser.test_scored
  group by genre, I_genre;
quit;

/* 2) TP, actual totals, predicted totals */
proc fedsql sessref=mySession;

  create table casuser.tp as
  select actual as class, sum(n) as TP
  from casuser.cm
  where actual = predicted
  group by actual;

  create table casuser.actual_tot as
  select actual as class, sum(n) as actual_n
  from casuser.cm
  group by actual;

  create table casuser.pred_tot as
  select predicted as class, sum(n) as pred_n
  from casuser.cm
  group by predicted;

  create table casuser.class_counts as
  select
    a.class,
    coalesce(t.TP,0) as TP,
    (a.actual_n - coalesce(t.TP,0)) as FN,
    (p.pred_n   - coalesce(t.TP,0)) as FP
  from casuser.actual_tot a
  left join casuser.tp t
    on a.class = t.class
  left join casuser.pred_tot p
    on a.class = p.class;

quit;

/* 3) per-class precision/recall/f1 */
data casuser.class_metrics;
  set casuser.class_counts;
  if TP+FP > 0 then precision = TP/(TP+FP); else precision=.;
  if TP+FN > 0 then recall    = TP/(TP+FN); else recall=.;
  if precision+recall > 0 then f1 = 2*(precision*recall)/(precision+recall);
  else f1=.;
run;

/* 4) macro averages */
proc means data=casuser.class_metrics noprint;
  var precision recall f1;
  output out=casuser.macro mean=precision recall f1;
run;

/* 5) final 1-row table */
data casuser.lyrics_metrics_table;
  merge casuser.acc casuser.macro;
  length Model $40;
  Model = "Lyrics-only";
  keep accuracy precision recall f1;
run;

/* 6) print with title */
title "Lyrics-only Model Performance Metrics";
proc print data=casuser.lyrics_metrics_table label noobs;
  label accuracy="Accuracy" precision="Precision" recall="Recall" f1="F1 Score";
  format accuracy precision recall f1 8.4;
run;
title;

/* build multimodal base table */
/* 1) Multimodal base table: requires both audio + lyrics */
proc casutil;
  droptable incaslib="casuser" casdata="spotify_mm_base" quiet;
  droptable incaslib="casuser" casdata="spotify_mm_split" quiet;
quit;

proc fedsql sessref=mySession;
  create table casuser.spotify_mm_base as
  select
    genre,
    lyrics,
    danceability, energy, valence, speechiness, acousticness,
    instrumentalness, liveness, loudness, tempo, duration_ms,
    key, mode
  from casuser.spotify_songs_clean
  where genre is not null
    and lyrics is not null
    and length(trim(lyrics)) > 0
    and danceability is not null
    and energy is not null
    and valence is not null
    and speechiness is not null
    and acousticness is not null
    and instrumentalness is not null
    and liveness is not null
    and loudness is not null
    and tempo is not null
    and duration_ms is not null
    and key is not null
    and mode is not null
  ;
quit;

/* 2) Stratified split ONCE (keeps audio+lyrics aligned) */
proc cas;
  sampling.stratified /
    table   ={name="spotify_mm_base", caslib="casuser"}
    target  ="genre"
    samppct =70
    seed    =42
    partInd =true
    output  ={casout={name="spotify_mm_split", caslib="casuser", replace=true}, copyvars="ALL"};
quit;

/* check split */
proc fedsql sessref=mySession;
  select _PartInd1_, count(*) as n
  from casuser.spotify_mm_split
  group by _PartInd1_;
quit;

/* hashed lyric features in cas while keeping audio and partInd1 */
%let K = 500;

proc casutil;
  droptable incaslib="casuser" casdata="mm_hash_cas" quiet;
quit;

proc ds2 sessref=mySession;
  data casuser.mm_hash_cas (overwrite=yes);

    dcl varchar(10) genre;
    dcl int _PartInd1_;

    /* audio vars */
    dcl double danceability energy valence speechiness acousticness instrumentalness;
    dcl double liveness loudness tempo duration_ms key mode;

    /* lyrics */
    dcl varchar(32767) lyrics;
    dcl varchar(32767) lyrics_clean;
    dcl varchar(50) word;
    dcl varchar(32) hx;
    dcl int t h i;

    /* declare feat1-feat&K */
    %macro decl_feats(K);
      %do j=1 %to &K;
        dcl double feat&j;
      %end;
    %mend;
    %decl_feats(&K);

    dcl double x[&K];

    method run();
      set casuser.spotify_mm_split;

      lyrics_clean = lowcase(prxchange('s/[^a-z0-9 ]+/ /', -1, coalesce(lyrics,'')));

      do i=1 to &K; x[i]=0; end;

      t=1;
      do while(scan(lyrics_clean, t, ' ') ne '');
        word = scan(lyrics_clean, t, ' ');
        if length(word) >= 3 then do;
          hx = put(md5(word), $hex32.);
          h  = mod(inputn(substr(hx,1,8), 'HEX8.'), &K) + 1;
          x[h] = x[h] + 1;
        end;
        t=t+1;
      end;

      %macro write_feats(K);
        %do j=1 %to &K;
          feat&j = x[&j];
        %end;
      %mend;
      %write_feats(&K);

      output;
    end;

  enddata;
run;
quit;

/* train and score multimodel using gradboost */
proc gradboost data=casuser.mm_hash_cas;
  target genre / level=nominal;

  input danceability energy valence speechiness acousticness instrumentalness
        liveness loudness tempo duration_ms key mode
        feat1-feat&K / level=interval;

  partition rolevar=_PartInd1_(train='1' validate='0');

  score out=casuser.mm_scored copyvars=(genre _PartInd1_);
run;

/* metrics on test set */
/* test set only + correct flag */
data casuser.mm_test_scored;
  set casuser.mm_scored;
  where _PartInd1_ = 0;
  correct = (strip(genre) = strip(I_genre));
run;

/* accuracy */
proc means data=casuser.mm_test_scored noprint;
  var correct;
  output out=casuser.mm_acc mean=accuracy;
run;

/* confusion counts */
proc fedsql sessref=mySession;
  create table casuser.mm_cm as
  select
    genre   as actual,
    I_genre as predicted,
    count(*) as n
  from casuser.mm_test_scored
  group by genre, I_genre;
quit;

/* tp / totals / fp fn */
proc fedsql sessref=mySession;

  create table casuser.mm_tp as
  select actual as class, sum(n) as TP
  from casuser.mm_cm
  where actual = predicted
  group by actual;

  create table casuser.mm_actual_tot as
  select actual as class, sum(n) as actual_n
  from casuser.mm_cm
  group by actual;

  create table casuser.mm_pred_tot as
  select predicted as class, sum(n) as pred_n
  from casuser.mm_cm
  group by predicted;

  create table casuser.mm_class_counts as
  select
    a.class,
    coalesce(t.TP,0) as TP,
    (a.actual_n - coalesce(t.TP,0)) as FN,
    (p.pred_n   - coalesce(t.TP,0)) as FP
  from casuser.mm_actual_tot a
  left join casuser.mm_tp t on a.class=t.class
  left join casuser.mm_pred_tot p on a.class=p.class;

quit;

/* precision/recall/f1 per class */
data casuser.mm_class_metrics;
  set casuser.mm_class_counts;
  if TP+FP > 0 then precision = TP/(TP+FP); else precision=.;
  if TP+FN > 0 then recall    = TP/(TP+FN); else recall=.;
  if precision+recall > 0 then f1 = 2*(precision*recall)/(precision+recall);
  else f1=.;
run;

/* macro averages */
proc means data=casuser.mm_class_metrics noprint;
  var precision recall f1;
  output out=casuser.mm_macro mean=precision recall f1;
run;

/* final table */
data casuser.mm_metrics_table;
  merge casuser.mm_acc casuser.mm_macro;
  length Model $60;
  Model = "Multimodal (Audio + Lyrics)";
  keep accuracy precision recall f1;
run;

title "Multimodal Model Performance Metrics";
proc print data=casuser.mm_metrics_table label noobs;
  label accuracy="Accuracy" precision="Precision" recall="Recall" f1="F1 Score";
  format accuracy precision recall f1 8.4;
run;
title;

/* visualize metrics */
/* combine three model metrics tables */
/* pull CAS tables to WORK (safe even if already in WORK) */
data work.lyrics_metrics;
  set casuser.lyrics_metrics_table;
run;

data work.mm_metrics;
  set casuser.mm_metrics_table;
run;

/* keep just model + accuracy, stack into one table */
data work.model_acc;
  length Model $40;
  set work.audio_model_metrics(in=a)
      work.lyrics_metrics(in=l)
      work.mm_metrics(in=m);
  keep Model Accuracy;
run;

/* check if work model acc exists */
proc contents data=work.model_acc; run;
proc print data=work.model_acc; run;


/* bar chart for three models' accuracies */
title "Accuracy Comparison Across Models (Test Set)";
proc sgplot data=work.model_acc;
  vbar Model / response=Accuracy datalabel;
  yaxis grid label="Accuracy" values=(0 to 1 by 0.1);
  xaxis display=(nolabel);
  format Accuracy 8.4;
run;
title;

/* correct v. incorrect predictions */
/*---------------------------------------------*/
/* MULTIMODAL: Correct vs Incorrect by Genre   */
/*---------------------------------------------*/

data work.mm_test_flag;
  set casuser.mm_scored;
  where _PartInd1_ = 0; /* test */
  length correct $9;
  correct = ifc(strip(genre)=strip(I_genre), "Correct", "Incorrect");
run;

proc freq data=work.mm_test_flag noprint;
  tables genre*correct / out=work.mm_correct_counts(drop=percent);
run;

title "Audio + Lyrics Model: Correct vs Incorrect Predictions by Genre (Test Set)";
proc sgplot data=work.mm_correct_counts;
  vbar genre / response=count group=correct groupdisplay=cluster datalabel;
  xaxis display=(nolabel) fitpolicy=rotate;
  yaxis grid label="Number of Songs";
run;
title;