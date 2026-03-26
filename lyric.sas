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

/* correct v. incorrect classifications */
/* 1) Test set only + correctness flag */
data casuser.lyrics_eval;
  set casuser.lyrics_pred;
  where _PartInd1_ = 0;

  length correct $9;
  if strip(genre) = strip(pred_genre) then correct="Correct";
  else correct="Incorrect";
run;

/* 2) Aggregate counts */
proc fedsql sessref=mySession;
  create table casuser.lyrics_correct_counts as
  select
    genre,
    correct,
    count(*) as n
  from casuser.lyrics_eval
  group by genre, correct;
quit;

/* 3) Plot */
data work.lyrics_correct_counts;
  set casuser.lyrics_correct_counts;
run;

title "Lyrics-only Model: Correct vs Incorrect Predictions by Genre (Test Set)";
proc sgplot data=work.lyrics_correct_counts;
  vbar genre / response=n group=correct groupdisplay=cluster datalabel;
  xaxis display=(nolabel) fitpolicy=rotate;
  yaxis grid label="Number of Songs";
run;
title;