# Call Report curation details

This article collects lower-level details about parsing FFIEC source
files and evaluating the resulting Parquet data against a curation
service-level agreement.

## 1 The boring details

Now that I have explained how you can use the curated data, I will spend
a little time explaining the data curation process. I focus on the more
challenging aspects and probably don’t fail to deliver on the
description “boring” in this section.

### 1.1 Reading the data

Each quarter’s zip file (`zipfile`) actually contains dozens of text
files (`.txt`) in TSV (“tab-separated values”) form. The TSV is a close
relative of the CSV (“comma-separated values”) and the principles
applicable to one form apply to the other.

I have seen code that just imports from these individual files (what I
call `inner_file`) in some location on the user’s hard drive. This
approach is predicated on the user having downloaded the zip files and
unzipped them. While we have downloaded all the zip files—assuming you
followed the steps outlined in the [raw data
section](https://iangow.github.io/ffiec.pq/articles/curate-call-reports.html#getting-the-raw-data)—I
don’t want to be polluting my hard drive (or yours) with thousands of
`.txt` files that won’t be used after reading them once.

Instead, R allows me to simply say:

``` r

con <- unz(zipfile, inner_file)
```

The resulting `con` object is a temporary read-only connection to a
single text file (`inner_file`) stored inside the zip file `zipfile`.
The object allows R to stream the file’s contents directly from the zip
archive, line by line, without extracting it. Given `con`, the core
function used to read the data into R has the following basic form,
where `read_tsv()` comes from the `readr` package, part of the
Tidyverse:

``` r

df <- read_tsv(
  con,
  col_names = cols,
  col_types = colspec,
  skip = skip,
  quote = "",
  na = c("", "CONF"),
  progress = FALSE,
  show_col_types = FALSE
)
```

#### 1.1.1 Handling embedded newlines and tabs

Experienced users of the `readr` package might wince a little at the
`quote = ""` argument above. What this means is that the data are not
quoted. Wickham et al. ([2023, p. 101](#ref-Wickham:2023aa)) points out
that “sometimes strings in a CSV file contain commas. To prevent them
from causing problems, they need to be surrounded by a quoting
character, like `"` or `'`. By default, `read_csv()` assumes that the
quoting character will be `"`.”

Adapting this to our context and expanding it slightly, I would say:
“sometimes strings in a TSV file contain tabs (`\t`) and newline
characters (`\n`).[^1] To prevent them from causing problems, they need
to be surrounded by a quoting character, like `"` or `'`.” While this is
a true statement, the TSV files provided on the FFIEC Bulk Data website
are *not* quoted, which means that tabs and newlines characters
**embedded** in strings *will* cause problems.

The approach taken by the `ffiec.pq` package is to attempt to read the
data using a call like that above, which I term the “fast path” (in part
because it is indeed fast). Before making that call, the code has
already inspected the first row of the file to determine the column
names (stored in `cols`) and used those column names to look up the
appropriate type for each column (stored in `colspecs`). Any anomaly
caused by embedded newlines or embedded tabs will almost certainly cause
this first `read_tsv()` call to fail. But if there are no issues, then
we pretty much have the data as we want them and can return `df` to the
calling function.[^2] Fortunately, over 95% of files can be read
successfully on the “fast path”.

It turns out that if the “fast path” read fails, the most likely culprit
is **embedded newlines**. Let’s say the table we’re trying to read has
seven columns and the text field that is the fourth field in the file
contains, in some row of the data, an embedded newline, because the data
submitted by the reporting financial institution contained `\n` in that
field. Because `read_tsv()` processes the data line by line and lines
are “delimited” by newline characters (`\n`), it will see the
problematic line as terminating part way through the fourth column and,
because `cols` tells `read_tsv()` to expect seven columns, `read_tsv()`
will issue a warning.

When a warning occurs on the “fast path”, the read function in
`ffiec.pq` moves to what I call (unimaginatively) the “slow path”. A
“feature” (it turns out) of the TSV files provided on the FFIEC Bulk
Data website is that each line ends with not just `\n`, but `\t\n`. This
means we can assume that any `\n` not preceded by `\t` is an embedded
newline, not a line-terminating endline.[^3] So I can read the data into
the variable `txt` using
[`readLines()`](https://rdrr.io/r/base/readLines.html) and use a regular
expression to replace embedded newlines with an alternative character.
The alternative I use is a space and I use the
[`gsub()`](https://rdrr.io/r/base/grep.html) function to achieve this:
`gsub("(?<!\\t)\\n", " ", txt, perl = TRUE)` The regular expression here
is `(?<!\\t)\\n` is equivalent to `(?<!\t)\n` in Perl or `r"(?<!\t)\n"`
in Python.[^4] In words, the regular expression literally means “any
newline character that is not immediately preceded by a tab” and the
[`gsub()`](https://rdrr.io/r/base/grep.html) function will replace such
characters with spaces (`" "`) because that is the second argument to
[`gsub()`](https://rdrr.io/r/base/grep.html).

This fix addresses almost all the problematic files. “Almost all” means
“all but two”. The issue with the remaining two files is (as you might
have guessed) **embedded tabs**. Unfortunately, there’s no easy
“identify the embedded tabs and replace them” fix, because there’s no
easy way to distinguish embedded tabs from delimiting tabs. However, we
can detect the presence of embedded tabs in an affected from the
existence of too many tabs in that row.

For one of the two “bad” files, there is only one text field, so once we
detect the presence of the embedded tab, we can assume that the extra
tab belongs in that field: Problem solved. For the other “bad” file,
there are several text fields and, while there is just one bad row, we
cannot be sure which text field has the embedded tab. The `ffiec.pq`
package just assumes that the embedded tab belongs in the last field and
moves on. This means that the textual data for one row (i.e., one
financial institution) for one schedule for one quarter cannot be
guaranteed to be completely correct.[^5] Such is life.

Even without addressing the issue, given that only two files are
affected, it’s possible to measure the “damage” created by embedded
tabs.

Let’s look at the earlier file, which turns out to affect the Schedule
RIE data for The Traders National Bank (`IDRSSD` of `490937`) for June
2004.[^6] Traders National Bank in Tennessee was “Tullahoma’s second
oldest bank”, until changing its name to Wellworth Bank (seemingly after
some mergers), and it listed total assets of \$117,335,000 in the
affected Call Report.

Let’s look at the textual data we have in our Parquet files for this
case:[^7]

``` r

ffiec_str <- ffiec_scan_pqs(db, schedule = "ffiec_str") 

ffiec_str |> 
  filter(IDRSSD == "490937", date == "2004-06-30") |> 
  select(IDRSSD, item, value) |> 
  filter(!is.na(value)) |>
  collect() |>
  system_time()
```

We can compare this with what we see in the Call Report extract below,
where we see that the value of `TEXT4468` should be something like
`"Courier, Audit Tax, Deff Comp, Other\tns Exp, Bus Dev, P"`. The
embedded tab has split this into
`"Courier, Audit Tax, Deff Comp, Other"` for `TEXT4468` and
`"ns Exp, Bus Dev, P"` for `TEXT4469`, which should be `NA`. If the
values for `TEXT4468`and `TEXT4469` for Traders National Bank in June
2004 are important to your analysis, you could fix this “by hand” easily
enough.

Figure 1: Extract from June 2004 Call Report for The Traders National
Bank

Looking at the later file, there were embedded tabs in two rows of
Schedule NARR for December 2022. I compared the values in the Parquet
file with those in the Call Reports for the two affected banks and the
values in the Parquet file match perfectly.[^8] Because Schedule NARR
(“Optional Narrative Statement Concerning the Amounts Reported in the
Consolidated Reports of Condition and Income”) has just one text column
(`TEXT6980`), the fix employed by the `ffiec.pq` package will work
without issues.[^9]

#### 1.1.2 Handling missing-value sentinels

Users familiar with both `readr` and Call Report data might also have
noticed the use of `na = c("", "CONF")` in the call to `read_tsv()`
above. The default value for this function is `na = c("", "NA")` means
that empty values and the characters `NA` are treated as missing values.
As I saw no evidence that the export process for FFIEC Bulk Data files
used `"NA"` to mark `NA` values, I elected not to treat `"NA"` as `NA`.
However, a wrinkle is that the reporting firms some times populate text
fields—but not numerical fields—with the value `"NA"`.[^10] While the
most sensible interpretation of such values is as `NA`, without further
investigation it is difficult to be sure that `"NA"` is the canonical
form in which firms reported `NA` values rather than `"N/A"` or
`"Not applicable"` or some other variant.

This approach seems validated by the fact that I see the value `"NR"` in
text fields of PDF versions of Call Reports and these values show up as
empty values in the TSV files, suggesting that `"NR"`, not `"NA"` is the
FFIEC’s canonical way of representing `NA` values in these files, while
`"NA"` is literally the text value `"NA"`, albeit perhaps one intended
*by the reporting firm* to convey the idea of `NA`. Users of the FFIEC
data created by the `ffiec.pq` package who wish to use textual data
should be alert to the possibility that values in those fields may be
intended by the reporting firm to convey the idea of `NA`, even if they
are not treated as such by the FFIEC’s process for creating the TSV
files.

The other value in the `na` argument used above is `"CONF"`, which
denotes that the the reported value is confidential and therefore not
publicly disclosed. Ideally, we might distinguish between `NA`, meaning
“not reported by the firm to the FFIEC” or “not applicable to this firm”
or things like that, from `"CONF"`, meaning the FFIEC has the value, but
we do not. Unfortunately, the value `"CONF"` often appears in numeric
fields and there is no simple way to ask `read_tsv()` to record the idea
that “this value is confidential”, so I just read these in as `NA`.[^11]

I say “no simple way” because there are probably workarounds that allow
`"CONF"` to be distinguished from true `NA`s. For example, I could have
chosen to have `read_tsv()` read all numeric fields as character fields
and then convert the value `CONF` in such fields to a **sentinel value**
such as `Inf` (R’s way of saying “infinity” or $`\infty`$).[^12] This
would not be terribly difficult, but would have the unfortunate effect
of surprising users of the data who (understandably) didn’t read the
manual and starting finding that the mean values of some fields are
`Inf`. Perhaps the best way to address this would allow the user of
`ffiec.pq` to *choose* that behaviour as an option, but I did not
implement this feature at this time.

In addition to these missing values, I discovered in working with the
data that the FFIEC often used specific values as **sentinel values**
for `NA`. For example, `"0"` is used for some fields, while `"00000000"`
is used to mark dates as missing, and `"12/31/9999 12:00:00 AM"` is used
for timestamps.[^13] I recoded such sentinel values as `NA` in each
case.

## 2 The service-level agreement

Gow ([2026](#ref-Gow:2026aa)) suggested a pro forma service-level
agreement covering the deliverables from the *Curate* team with the
following elements:

1.  The data will be presented as a set of tables in a modern storage
    format.
2.  The division into tables will adhere to a pragmatic version of good
    database principles.
3.  The **primary key** of each table will be identified and validated.
4.  Each variable (column) of each table will be of the correct type.
5.  There will be no manual steps that cannot be reproduced.
6.  A process for updating the curated data will be established.
7.  The entire process will be documented in some way.
8.  Some process for version control of data will be maintained.

So in this section, I do an evaluation (perhaps biased) of how well
`ffiec.pq` meets the requirements of these elements.

### 2.1 Storage format

In principle, the storage format should fairly minor detail determined
by the needs of the *Understand* team. For example, if the *Understand*
team works in Stata or Excel, then perhaps they will want the data in
some kind of Stata format or as Excel files. However, I think it can be
appropriate to push back on notions that data will be delivered in form
that involves downgrading the data or otherwise compromises the process
in a way that may ultimately add to the cost and complexity of the task
for the *Curate* team. For example, “please send the final data as an
Excel file attachment as a reply email” might be a request to be
resisted because the process of converting to Excel can entail the
degradation of data (e.g., time stamps or encoding of text).[^14]
Instead it may be better to choose a more robust storage format and
supply a script for turning that into a preferred format.

One storage format that I have used in the past would deliver data as
tables in a (PostgreSQL) database. The *Understand* team could be given
access data from a particular source organized as a **schema** in a
database. Accessing the data in this form is easy for any modern
software package. One virtue of this approach is that the data might be
curated using, say, Python even though the client will analyse it using,
say, Stata.[^15] I chose to use Parquet files for `ffiec.pq`, in part
because I don’t have a PostgreSQL server to put the data into and share
with you. But Parquet files offer high performance, are space-efficient,
and can be used with any modern data analysis tool.

### 2.2 Good database principles

While I argued that one does not want to get “particularly fussy about
database normalization”, if anything I may have pushed this further than
some users might like. However, with
[`ffiec_pivot()`](https://iangow.github.io/ffiec.pq/reference/ffiec_pivot.md),
it is relatively easy (and not too costly) to get the data into a “wide”
form if that is preferred. The legacy version of Call Reports data
offered by WRDS went to the other extreme with a “One Big Table”
approach, which meant that this data set never moved to PostgreSQL
because of limits there.[^16]

### 2.3 Primary keys

In Gow ([2026](#ref-Gow:2026aa)), I suggested that “the *Curate* team
should communicate the primary key of each table to the *Understand*
team. A primary key of a table will be a set of variables that can be
used to uniquely identify a row in that table. In general a primary key
will have no missing values. Part of data curation will be confirming
that a proposed primary key is in fact a valid primary key.”

Table 1: Primary key checks

The primary-key code above uses
[`ffiec_check_pq_keys()`](https://iangow.github.io/ffiec.pq/reference/ffiec_check_pq_keys.md)
to check the validity of a proposed primary key for a schedule. That
every column except `value` forms part of the primary key is what allows
us to use
[`ffiec_pivot()`](https://iangow.github.io/ffiec.pq/reference/ffiec_pivot.md)
to create unique values in the resulting “wide” tables.

### 2.4 Data types

In Gow ([2026](#ref-Gow:2026aa)), I proposed that “each variable of each
table should be of the correct type. For example, dates should be of
type `DATE`, variables that only take integer values should be of
`INTEGER` type. Date-times should generally be given with
`TIMESTAMP WITH TIME ZONE` type. Logical columns should be supplied with
type `BOOLEAN`.”[^17]

This element is (to the best of my knowledge) satisfied with one
exception. The Parquet format is a bit like the Model T Ford: it
supports time zones, and you can use any time zone you want, so long as
it is UTC.[^18] As discussed above, there is only one timestamp in the
whole set-up, `last_date_time_submission_updated_on` on the POR files
and I discussed this field above.

### 2.5 No manual steps

When data vendors are providing well-curated data sets, much about the
curation process will be obscure to the user. This makes some sense, as
the data curation process has elements of trade secrets. But often data
will be supplied by vendors in an imperfect state and significant data
curation will be performed by the *Curate* team working for or within
the same organization as the *Understand* team.

Focusing on the case where the data curation process transforms an
existing data set—say, one purchased from an outside vendor—into a
curated data set in sense used here, there are a few ground rules
regarding manual steps.

“First, *the original data files should not be modified in any way*.”
Correct. The `ffiec.pq` package does not modify the FFIEC Bulk Data
files after downloading them. I do make some corrections to the
`item_name` variable in the `ffiec_items` package, but these “manual
steps \[are\] extensively documented and applied in a transparent,
automated fashion.” The code for these steps can be found on [the GitHub
page](https://github.com/iangow/ffiec.pq/blob/main/data-raw/ffiec_dict.R)
for the `ffiec.pq` package.

### 2.6 Documentation

“The process of curating the data should be documented sufficiently well
that someone else could perform the curation steps should the need
arise.” I regard that having the `ffiec.pq` package do all the work of
processing the data satisfies this requirement.

A important idea here is that the code for processing the data is
documentation in its own right. Beyond that the document you are reading
now is a form of documentation, as is the documentation in the
`ffiec.pq` package.

### 2.7 Update process

If a new zip file appears on the FFIEC Bulk Data website, you can
download it using the process outlined in the [raw data
section](https://iangow.github.io/ffiec.pq/articles/curate-call-reports.html#getting-the-raw-data).
Just changing the `[:4]` to `[0]` and the script downloads the latest
file.

Then run the following code and the data will be updated:

``` r

results <-
  ffiec_list_zips() |>
  filter(date == max(date)) |>
  select(zipfile) |>
  pull() |>
  ffiec_process() |>
  system_time()

results |> count(date, ok)
```

### 2.8 Data version control

Welch ([2019](#ref-Welch:2019aa)) argues that, to ensure that results
can be reproduced, “the author should keep a private copy of the full
data set with which the results were obtained.” This imposes a
significant cost on the *Understand* team to maintain archives of data
sets that may run to several gigabytes or more and it would seem much
more efficient for these obligations to reside with the parties with the
relevant expertise. Data version control is a knotty problem and one
that even some large data providers don’t appear to have solutions for.

I am delivering the Call Report data not as the data files, but as an R
package along with instructions for obtaining the zip files from the
FFIEC Bulk Data website. So I cannot be said to be providing much
version control of data here. That said, if a user retains the
downloaded zip files, the application of the `ffiec.pq` functions to
process these into Parquet files should provide a high degree of
reproducibility of the data for an individual researcher.[^19]

For my own purposes, I achieve a *modest* level of data version control
by using Dropbox, which offers the ability to restore some previous
versions of data files.

## 3 The future of data curation

Data science seems to be in a strange state at the moment. On the one
hand, things must be a little depressing. Many data scientists must be
wondering about their future in a world in which AI can handle many
tasks formerly done by people. On the other hand, data science has come
leaps and bounds in the last fifteen years or so (pandas doesn’t even
rate a mention in [Janert, 2010](#ref-janert2010data)) and things like
Polars, and Arrow, and DuckDB, not to mention things like the Tidyverse
are truly exciting developments.

My sense from working on this data curation task is that humans will
continue to be essential for data curation for the foreseeable future.
In fact, the value of data curation might have gone up, as AI increase
the population of people whose modest coding skills no longer prevent
them from trying their hand at data analysis.

While I used OpenAI’s ChatGPT 5.2 on this project, there were some “open
the pod-bay doors, please, HAL” moments along the way. The one-line
`txt2 <- gsub("(?<!\\t)\\n", " ", txt, perl = TRUE)` fix was deemed
“unsafe” and ChatGPT even passive-aggressively suggested that “I think
you want to use `txt`, not `txt2` in `read_tsv()`” at one point. I think
ChatGPT did not understand that this code is not being put into a
production system in which users load random zip files at all hours of
the day. In effect, I have the population of zip files and, because of
how I wrote the code, any errors in reading would’ve have just stopped
the code. Using that one-liner allowed me to delete 700 lines of sloppy
AI code. That said, I’m not sure I would have attempted this task if AI
weren’t along for the ride.

Gow, I.D., 2026. [Data curation and the data science
workflow](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6149488).

Janert, P.K., 2010. Data analysis with open source tools: A hands-on
guide for programmers and data scientists. O’Reilly Media, Sebastopol,
CA.

Welch, I., 2019. Editorial: An opinionated FAQ. Critical Finance Review
8, 19–24. <https://doi.org/10.1561/104.00000077>

Wickham, H., Çetinkaya-Rundel, M., Grolemund, G., 2023. [R for data
science](https://books.google.com/books?id=TiLEEAAAQBAJ). O’Reilly
Media, Sebastopol, CA.

[^1]: Tabs and newlines are what are sometimes called **invisibles**
    because their presence is not apparent from viewing their usual
    representation as text (for example, a tab might look the same as a
    series of spaces). The `\t` and `\n` representations are quite
    standard ways of making these characters visible to humans.

[^2]: In practice, there’s a little clean-up to be done before returning
    `df`, as I will explain shortly.

[^3]: Unfortunately, the `read_tsv()` function does not allow us to
    specify an alternative to the default for line-terminating
    characters. It seems that other R and Python packages also do not
    offer this option.

[^4]: There are extra backslashes in the R version to specify that the
    backslashes represent backslashes to be passed along to
    [`gsub()`](https://rdrr.io/r/base/grep.html), not special characters
    to be interpreted by R itself.

[^5]: This is not a completely insoluble problem in that we could
    inspect the XBRL data to determine the correct form of the data in
    this case. This is “above my pay grade” in this setting. (I’m not
    being paid to do this!)

[^6]: Note that it would be somewhat faster to use
    `ffiec_text <- load_parquet(db, "ffiec_str_20040630", "ffiec")`, but
    this code doesn’t take too long to run.

[^7]: Only the textual data will have embedded tabs and, because such
    data is arranged after numerical data, only text data will be
    affected by embedded tabs.

[^8]: See [here](https://github.com/iangow/ffiec.pq/issues/3) for the
    gory details. Interestingly, the WRDS Call Report data have the
    [same
    issue](https://gist.github.com/iangow/809e67527c41dd54872f155b67f6c442)
    with the earlier case and have incorrect data for one of the banks
    in the latter case. This seems to confirm that WRDS uses the TSV
    data itself in creating its Call Report data sets.

[^9]: Recall that the “fix” assumes that embedded tab belongs in last
    available text column.

[^10]: If `"NA"` appeared in a numeric field, my code would report an
    error. As I detected no errors in importing the data, I know there
    are no such values.

[^11]: This is the kind of situation where SAS’s approach to coding
    missing values would be helpful.

[^12]: In a sense, this would be doing the opposite of what the Python
    package pandas did in treating `np.NaN` as the way of expressing
    what later became `pd.NA`; I’d be using `Inf` to distinguish
    different kinds of missing values.

[^13]: Note that this timestamp sentinel appears in the “MDRM” data from
    the Federal Reserve that I used to construct `ffiec_items`, not in
    the FFIEC data sets themselves.

[^14]: I discuss some of the issues with Excel as a storage format
    below.

[^15]: One project I worked on involved Python code analysing text and
    putting results in a PostgreSQL database and a couple of lines of
    code were sufficient for a co-author in a different city to load
    these data into Stata.

[^16]: A rule of thumb might be that, if you cannot store your fairly
    standard data in PostgreSQL, then perhaps you need to revisit the
    structure of the data.

[^17]: Gow ([2026](#ref-Gow:2026aa)) is referring to PostgreSQL types.
    The `ffiec.pq` package uses (logical) Parquet types `DATE`, `INT32`,
    `TIMESTAMP(isAdjustedToUTC = true)`, and `BOOLEAN` types,
    respectively for these types. Floating-point numbers are stored as
    `FLOAT64` and strings as `STRING`.

[^18]: Henry Ford famously said of the Model T that “any customer can
    have a car painted any color that he wants so long as it is black.”
    Strictly speaking, the Parquet format supports **timezone-aware
    timestamps**, but only as UTC instants, as other time zones are not
    supported.

[^19]: Note that a researcher might need to use a specific version of
    the `ffiec.pq` package to achieve full reproducibility, but the
    `pak` package allows for that.
