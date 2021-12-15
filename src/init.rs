pub const INITSQL : &str = "

CREATE FN [sys].[AddColumn]( t int, name string, typ int ) 
AS 
BEGIN 
  INSERT INTO sys.Column( Table, Name, Type ) VALUES (t, name, typ)
END
GO
CREATE FN [sys].[ClearTable](t int) AS 
BEGIN 
  EXECUTE( 'DELETE FROM ' | sys.TableName(t) | ' WHERE true' )
END
GO
CREATE FN [sys].[ColName]( table int, colId int ) RETURNS string AS
BEGIN
  DECLARE i int
  SET i = 0
  FOR result = Name FROM sys.Column WHERE Table = table
  BEGIN
    IF i = colId RETURN result
    SET i = i + 1
  END
  RETURN '?bad colId?'  
END
GO
CREATE FN [sys].[ColNames]( table int ) RETURNS string AS
BEGIN
  DECLARE col string
  SET result = '(Id'
  FOR col = Name FROM sys.Column WHERE Table = table
    SET result |= ',' | sys.QuoteName(col)
  RETURN result | ')'
END
GO
CREATE FN [sys].[ColValues]( table int ) RETURNS string AS
BEGIN
  DECLARE col string
  SET result = 'Id'
  FOR col = CASE 
    WHEN Type % 8 = 2 THEN 'sys.SingleQuote(' | Name | ')'
    ELSE Name
  END
  FROM sys.Column WHERE Table = table
    SET result |= '|'',''|' | col
  RETURN result
END
GO
CREATE FN [sys].[Cols]( table int ) RETURNS string AS
BEGIN
  DECLARE col string, list string
  FOR col = sys.QuoteName(Name) | ' ' | sys.TypeName(Type)
  FROM sys.Column WHERE Table = table
    SET list |= CASE WHEN  list = '' THEN col ELSE ',' | col END
  RETURN '(' | list | ')'
END
GO
CREATE FN [sys].[Dot]( schema string, name string ) RETURNS string AS
BEGIN
  RETURN sys.QuoteName( schema ) | '.' | sys.QuoteName( name )
END
GO
CREATE FN [sys].[DropColumn]( t int, cname string ) AS 
BEGIN 
  DELETE FROM sys.Column WHERE Table = t AND Name = cname
END
GO
CREATE FN [sys].[DropIndex]( ix int ) AS
BEGIN
  /* Note: this should not be called directly, instead use DROP INDEX statement */
  DELETE FROM sys.IndexColumn WHERE Index = ix
  DELETE FROM sys.Index WHERE Id = ix
END
GO
CREATE FN [sys].[DropSchema]( sid int ) AS
/* Note: this should not be called directly, instead use DROP SCHEMA statement */
BEGIN
  DECLARE schema string, name string
  SET schema = Name FROM sys.Schema WHERE Id = sid
  FOR name = Name FROM sys.Function WHERE Schema = sid EXECUTE( 'DROP FN ' | sys.Dot(schema,name) )
  FOR name = Name FROM sys.Table WHERE Schema = sid EXECUTE( 'DROP TABLE ' | sys.Dot(schema,name) )
  DELETE FROM sys.Schema WHERE Id = sid
END
GO
CREATE FN [sys].[DropTable]( t int ) AS 
/* Note: this should not be called directly, instead use DROP TABLE statement */
BEGIN
  /* Delete the rows */
  EXECUTE( 'DELETE FROM ' | sys.TableName(t) | ' WHERE true' )

  DECLARE id int
  /* Delete the Index data */
  FOR id = Id FROM sys.Index WHERE Table = t
  BEGIN
    DELETE FROM sys.IndexColumn WHERE Index = id
  END
  DELETE FROM sys.Index WHERE Table = t
   /* Delete the column data */
  FOR id = Id FROM sys.Column WHERE Table = t
  BEGIN
    DELETE FROM browse.Column WHERE Id = id
  END
  /* Delete other data */
  DELETE FROM browse.Table WHERE Id = t
  DELETE FROM sys.Column WHERE Table = t
  DELETE FROM sys.Table WHERE Id = t
END
GO
CREATE FN [sys].[IndexCols]( index int ) RETURNS string AS
BEGIN
  DECLARE table int, list string, col string
  SET table = Table FROM sys.Index WHERE Id = index
  FOR col = sys.QuoteName(sys.ColName( table, ColId )) FROM sys.IndexColumn WHERE Index = index
    SET list |= CASE WHEN  list = '' THEN col ELSE ',' | col END
  RETURN '(' | list | ')'
END
GO
CREATE FN [sys].[IndexName]( index int ) RETURNS string AS
BEGIN
  SET result = sys.QuoteName(Name) FROM sys.Index WHERE Id = index
END
GO
CREATE FN [sys].[LoadAllTables]() AS BEGIN 

  DECLARE sid int, sname string, tname string
  FOR sid = Id, sname = Name FROM sys.Schema
  BEGIN
    FOR tname = Name FROM sys.Table WHERE Schema = sid
    BEGIN
      EXECUTE( 'IF false SELECT Id FROM ' | sys.Dot( sname, tname ) )
    END
  END

END
GO
CREATE FN [sys].[ModifyColumn]( t int, cname string, typ int ) AS 
BEGIN 
   UPDATE sys.Column SET Type = typ WHERE Table = t AND Name = cname
END
GO
CREATE FN [sys].[QuoteName]( s string ) RETURNS string AS
BEGIN
  RETURN '[' | REPLACE( s, ']', ']]' ) | ']'
END
GO
CREATE FN [sys].[SchemaName]( schema int) RETURNS string AS 
BEGIN 
  SET result = Name FROM sys.Schema WHERE Id = schema
END
GO
CREATE FN [sys].[ScriptBrowse]( t int ) AS
BEGIN
  -- Script browse information for Table t.
  -- Looks up Table and Column Id values (tid,cid) by name in case they change.
  DECLARE sid int, tname string, sname string
  SET sid = Schema, tname = Name FROM sys.Table WHERE Id = t
  SET sname = Name FROM sys.Schema WHERE Id = sid

  SELECT '
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = ' | sys.SingleQuote(sname) | '
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = ' | sys.SingleQuote(tname) 
SELECT '
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'
    | sys.SingleQuote(NameFunction) |','|sys.SingleQuote(SelectFunction) 
    | ',' | sys.SingleQuote(DefaultOrder) | ',' | sys.SingleQuote(Title) | ',' 
    | sys.SingleQuote(Description) | ',' | Role | ')'
  FROM browse.Table WHERE Id = t

  DECLARE cid int, cname string
  FOR cid=Id, cname=Name FROM sys.Column WHERE Table = t
  BEGIN
    SELECT '
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = ' | sys.SingleQuote(cname) | '
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, '
      |Position|','|sys.SingleQuote(Label)
      |','|sys.SingleQuote(Description)
      |','|RefersTo|','|sys.SingleQuote(Default)|','|InputCols|','|sys.SingleQuote(InputFunction)
      |','|InputRows|','|Style|','|sys.SingleQuote(DisplayFunction)|','|sys.SingleQuote(ParseFunction)|')'
    FROM browse.Column WHERE Id = cid
  END
  SELECT '
GO'
END
GO
CREATE FN [sys].[ScriptSchema]( s int ) AS
BEGIN
  DECLARE sname string SET sname = sys.SchemaName(s)

  /* Create the schema, tables, indexes */
  
  IF sname != 'sys'
  BEGIN
    SELECT '
--############################################
CREATE SCHEMA ' | sys.QuoteName( sname )

    DECLARE t int
    FOR t = Id FROM sys.Table WHERE Schema = s ORDER BY Name
    BEGIN
      EXEC sys.ScriptTable(t)
    END
  END

  /******* Script functions *******/

  SELECT '
CREATE FN ' | sys.Dot( sname,Name) | Def | '
GO' 
  FROM sys.Function  WHERE Schema = s ORDER BY Name

  /******* Script Data *******/

  IF sname != 'sys' AND sname != 'browse'
  BEGIN
    DECLARE ins string, val string
    FOR ins = '
INSERT INTO ' | sys.TableName(Id) | sys.ColNames(Id) | ' VALUES 
',
        val = 'SELECT ''(''|' | sys.ColValues(Id) | '|'')
''' | ' FROM ' | sys.TableName(Id)
    FROM sys.Table WHERE Schema = s ORDER BY Name
    BEGIN
      SELECT ins
      EXECUTE( val )
      SELECT 'GO
'
    END
  END
END
GO
CREATE FN [sys].[ScriptSchemaBrowse]( s int ) AS
BEGIN
  DECLARE t int
  FOR t = Id FROM sys.Table WHERE Schema = s ORDER BY Name
  BEGIN
    EXEC sys.ScriptBrowse(t)
  END
END
GO
CREATE FN [sys].[ScriptTable]( t int ) AS
BEGIN
  SELECT '
CREATE TABLE ' | sys.TableName(t) | sys.Cols(t) | ' 
GO'
  DECLARE ix int, name string
  FOR ix = Id, name = Name FROM sys.Index WHERE Table = t
  BEGIN
    SELECT '
CREATE INDEX ' | sys.QuoteName(name) | ' ON ' | sys.TableName(t) | sys.IndexCols(ix) | '
GO'
  END
END
GO
CREATE FN [sys].[SingleQuote]( s string ) RETURNS string AS
BEGIN
  RETURN '''' | REPLACE( s, '''', '''''' ) | ''''
END
GO
CREATE FN [sys].[TableName]( table int ) RETURNS string AS
BEGIN
  DECLARE schema int, name string
  SET schema = Schema, name = Name FROM sys.Table WHERE Id = table
  IF name = '' RETURN ''
  SET result = sys.Dot( Name, name ) FROM sys.Schema WHERE Id = schema
END
GO
CREATE FN [sys].[TypeName]( t int ) RETURNS string AS 
BEGIN 
  DECLARE p int
  SET p = t / 8
  RETURN CASE 
    WHEN t = 0 THEN 'none'
    WHEN t = 13 THEN 'bool'
    WHEN t = 36 THEN 'float' 
    WHEN t = 68 THEN 'double'
    WHEN t = 67 THEN 'int'
    WHEN t = 129 THEN 'binary'
    WHEN t = 130 THEN 'string'
    ELSE 
    CASE 
       WHEN t % 8 = 1 THEN 'binary(' | (p-1) | ')'
       WHEN t % 8 = 2 THEN 'string(' | (p-1) | ')'
       WHEN t % 8 = 3 THEN 'int(' | p | ')'
       ELSE '???'
    END
  END
END
GO
--############################################
CREATE SCHEMA [date]
CREATE FN [date].[DaysToString]( date int ) RETURNS string AS
BEGIN
  RETURN date.WeekDayToString( 1 + (date+5) % 7 ) | ' ' | date.YearMonthDayToString( date.DaysToYearMonthDay( date ) )
END
GO
CREATE FN [date].[DaysToYearDay]( days int ) RETURNS int AS
BEGIN
  -- Given a date represented by the number of days since 1 Jan 0000
  -- calculate a date in Year/Day representation stored as
  -- year * 512 + day where day is 1..366, the day in the year.
  
  DECLARE year int, day int, cycle int
  -- 146097 is the number of the days in a 400 year cycle ( 400 * 365 + 97 leap years )
  SET cycle = days / 146097
  SET days = days - 146097 * cycle -- Same as days % 146097
  SET year = days / 365
  SET day = days - year * 365 -- Same as days % 365
  -- Need to adjust day to allow for leap years.
  -- Leap years are 0, 4, 8, 12 ... 96, not 100, 104 ... not 200... not 300, 400, 404 ... not 500.
  -- Adjustment as function of y is 0 => 0, 1 => 1, 2 =>1, 3 => 1, 4 => 1, 5 => 2 ..
  SET day = day - ( year + 3 ) / 4 + ( year + 99 ) / 100 - ( year + 399 ) / 400
  
  IF day < 0
  BEGIN
    SET year = year - 1
    SET day = day + CASE WHEN date.IsLeapYear( year ) THEN 366 ELSE 365 END
  END
  RETURN 512 * ( cycle * 400 + year ) + day + 1
END
GO
CREATE FN [date].[DaysToYearMonthDay]( days int ) RETURNS int AS
BEGIN
  RETURN date.YearDayToYearMonthDay( date.DaysToYearDay( days ) )
END
GO
CREATE FN [date].[IsLeapYear]( y int ) RETURNS bool AS
BEGIN
  RETURN y % 4 = 0 AND ( y % 100 != 0 OR y % 400 = 0 )
END
GO
CREATE FN [date].[MicroSecToString](micro int) RETURNS string AS
BEGIN
  DECLARE day int, sec int, min int, hour int
  SET sec = micro / 1000000
  SET day = sec / 86400 + 366 -- 86400 = 24 * 60 * 60, seconds in a day.
  SET sec = sec % 86400
  SET min = sec / 60
  SET sec = sec % 60
  SET hour = min / 60
  SET min = min % 60
  RETURN date.DaysToString(  day ) | ' ' | hour | ':' | min | ':' | sec
END
GO
CREATE FN [date].[MonthToString]( m int ) RETURNS string AS
BEGIN
  RETURN CASE
    WHEN m = 1 THEN 'Jan'
    WHEN m = 2 THEN 'Feb'
    WHEN m = 3 THEN 'Mar'
    WHEN m = 4 THEN 'Apr'
    WHEN m = 5 THEN 'May'
    WHEN m = 6 THEN 'Jun'
    WHEN m = 7 THEN 'Jul'
    WHEN m = 8 THEN 'Aug'
    WHEN m = 9 THEN 'Sep'
    WHEN m = 10 THEN 'Oct'
    WHEN m = 11 THEN 'Nov'
    WHEN m = 12 THEN 'Dec'
    ELSE '???'
  END
END
GO
CREATE FN [date].[NowString]() RETURNS string AS
BEGIN
  RETURN date.MicroSecToString( date.Ticks() )
END
GO
CREATE FN [date].[StringToDays]( s string ) RETURNS int AS
BEGIN
  -- Typical input is 'Feb 2 2020'
  DECLARE ms string, month int
  SET ms = SUBSTRING( s, 1, 3 )
  SET month = CASE 
    WHEN ms = 'Jan' THEN 1
    WHEN ms = 'Feb' THEN 2
    WHEN ms = 'Mar' THEN 3
    WHEN ms = 'Apr' THEN 4
    WHEN ms = 'May' THEN 5
    WHEN ms = 'Jun' THEN 6
    WHEN ms = 'Jul' THEN 7
    WHEN ms = 'Aug' THEN 8
    WHEN ms = 'Sep' THEN 9
    WHEN ms = 'Oct' THEN 10
    WHEN ms = 'Nov' THEN 11
    WHEN ms = 'Dec' THEN 12
    ELSE 0
  END  
  IF month = 0 THROW 'Unknown month parsing date ' | htm.Attr(ms)
  DECLARE six int -- Index of first space
  SET six = 4
  WHILE true
  BEGIN
    IF six > LEN(s) BREAK
    IF SUBSTRING( s, six, 1 ) = ' ' BREAK
    SET six = six + 1
  END
  DECLARE ssix int
  SET ssix = six+1
  WHILE true
  BEGIN
    IF ssix > LEN(s) BREAK
    IF SUBSTRING( s, ssix, 1 ) = ' ' BREAK
    SET ssix = ssix + 1
  END
 
  DECLARE day int, year int
  SET day = PARSEINT( SUBSTRING( s, six+1, ssix - six - 1) )
  IF day < 1 OR day > 31 THROW 'Day must be 1..31 parsing date ' | htm.Attr(''|day)
  SET year = PARSEINT( SUBSTRING( s, ssix + 1, LEN(s) ) )
  RETURN date.YearMonthDayToDays( date.YearMonthDay( year, month, day ) )
END
GO
CREATE FN [date].[StringToYearMonthDay]( s string ) RETURNS int AS
BEGIN
  RETURN date.DaysToYearMonthDay( date.StringToDays( s ) )
END
GO
CREATE FN [date].[Test]( y int, m int, d int, n int ) AS 
BEGIN
  DECLARE ymd int, days int
  SET ymd = date.YearMonthDay( y, m, d )
  SET days = date.YearMonthDayToDays( ymd )
  DECLARE i int
  SET i = 0
  WHILE i < n
  BEGIN
    SELECT '<br>' | date.DaysToString( days + i )
    SET i = i + 1
  END
END
GO
CREATE FN [date].[TestRoundTrip]() AS
BEGIN
  DECLARE day int
  SET day = 0
  WHILE day < 1000000
  BEGIN
    IF date.YearMonthDayToDays( date.DaysToYearMonthDay(day) ) != day
    BEGIN
      SELECT 'Test failed day = ' | day
      BREAK
    END
    SET day = day + 1
  END
  SELECT 'Finished test day=' | day | ' date=' | date.DaysToString(day)
END
GO
CREATE FN [date].[Ticks]() RETURNS int AS
BEGIN
  -- Microseconds since 1 Jan 0000
  RETURN GLOBAL(0) + 62135596800000000 /* 719162 * 24 * 3600 * 1000000 */
END
GO
CREATE FN [date].[Today]() RETURNS int AS
BEGIN
  DECLARE sec int, day int
  SET sec = date.Ticks() / 1000000
  SET day = sec / 86400 + 366
  RETURN day
END
GO
CREATE FN [date].[WeekDayToString]( wd int ) RETURNS string AS
BEGIN
  RETURN CASE
    WHEN wd = 1 THEN 'Mon'
    WHEN wd = 2 THEN 'Tue'
    WHEN wd = 3 THEN 'Wed'
    WHEN wd = 4 THEN 'Thu'
    WHEN wd = 5 THEN 'Fri'
    WHEN wd = 6 THEN 'Sat'
    WHEN wd = 7 THEN 'Sun'
    ELSE '?weekday?'
    END
END
GO
CREATE FN [date].[YearDay]( year int, day int ) RETURNS int AS
BEGIN
  RETURN year * 512 + day
END
GO
CREATE FN [date].[YearDayToDays]( yd int ) RETURNS int AS
BEGIN
  -- Given a date in Year/Day representation stored as y * 512 + d where 1 <= d <= 366 ( so d is day in year )
  -- returns the number of days since \"day zero\" (1 Jan 0000)
  -- using the Gregorian calendar where days divisible by 4 are leap years, except if divisible by 100, except if divisible by 400.
  DECLARE y int, d int, cycle int
  -- Extract y and d from yd.
  SET y = yd / 512, d = yd % 512 - 1
  SET cycle = y / 400, y = y % 400 -- The Gregorian calendar repeats every 400 years.
 
  -- Result days come from cycles, from years having at least 365 days, from leap years and finally d.
  -- 146097 is the number of the days in a 400 year cycle ( 400 * 365 + 97 leap years ).
  RETURN cycle * 146097 
    + y * 365 
    + ( y + 3 ) / 4 - ( y + 99 ) / 100 + ( y + 399 ) / 400
    + d
END
GO
CREATE FN [date].[YearDayToString]( yd int ) RETURNS string AS
BEGIN
   RETURN date.YearMonthDayToString( date.YearDayToYearMonthDay( yd ) )  
END
GO
CREATE FN [date].[YearDayToYearMonthDay]( yd int ) RETURNS int AS
BEGIN
  DECLARE y int, d int, leap bool, fdm int, m int, dim int
  SET y = yd / 512
  SET d = yd % 512 - 1
  SET leap = date.IsLeapYear( y )
  -- Jan = 0..30, Feb = 0..27 or 0..28  
  IF NOT leap AND d >= 59 SET d = d + 1
  SET fdm = CASE 
    WHEN d < 31 THEN 0 -- Jan
    WHEN d < 60 THEN 31 -- Feb
    WHEN d < 91 THEN 60 -- Mar
    WHEN d < 121 THEN 91 -- Apr
    WHEN d < 152 THEN 121 -- May
    WHEN d < 182 THEN 152 -- Jun
    WHEN d < 213 THEN 182 -- Jul
    WHEN d < 244 THEN 213 -- Aug
    WHEN d < 274 THEN 244 -- Sep
    WHEN d < 305 THEN 274 -- Oct
    WHEN d < 335 THEN 305 -- Nov
    ELSE 335 -- Dec
    END
  SET dim = d - fdm
  SET m = ( d - dim + 28 ) / 31
  RETURN date.YearMonthDay( y, m+1, dim+1 )
END
GO
CREATE FN [date].[YearMonthDay]( year int, month int, day int ) RETURNS int AS
BEGIN
  RETURN year * 512 + month * 32 + day
END
GO
CREATE FN [date].[YearMonthDayToDays]( ymd int ) RETURNS int AS
BEGIN
  RETURN date.YearDayToDays( date.YearMonthDayToYearDay( ymd ) )
END
GO
CREATE FN [date].[YearMonthDayToString]( ymd int ) RETURNS string AS
BEGIN
  DECLARE y int, m int, d int
  SET d = ymd % 32
  SET m = ymd / 32
  SET y = m / 16
  SET m = m % 16
  RETURN date.MonthToString(m) | ' ' | d | ' ' |  y
END
GO
CREATE FN [date].[YearMonthDayToYearDay]( ymd int ) RETURNS int AS
BEGIN
  DECLARE y int, m int, d int
  -- Extract y, m, d from ymd
  SET d = ymd % 32, m = ymd / 32  
  SET y = m / 16, m = m % 16
  -- Incorporate m into d ( assuming Feb has 29 days ).
  SET d = d + CASE
    WHEN m = 1 THEN 0 -- Jan
    WHEN m = 2 THEN 31 -- Feb
    WHEN m = 3 THEN 60 -- Mar
    WHEN m = 4 THEN 91 -- Apr
    WHEN m = 5 THEN 121 -- May
    WHEN m = 6 THEN 152 -- Jun
    WHEN m = 7 THEN 182 -- Jul
    WHEN m = 8 THEN 213 -- Aug
    WHEN m = 9 THEN 244 -- Sep
    WHEN m = 10 THEN 274 -- Oct
    WHEN m = 11 THEN 305 -- Nov
    ELSE 335 -- Dec
    END
  -- Allow for Feb being only 28 days in a non-leap-year.
  IF m >= 3 AND NOT date.IsLeapYear( y ) SET d = d - 1
  RETURN date.YearDay( y, d )
END
GO
--############################################
CREATE SCHEMA [htm]
CREATE FN [htm].[Attr]( s string ) RETURNS string AS
BEGIN
  SET s = REPLACE( s, '&', '&amp;' )
  SET s = REPLACE( s, '\"', '&quot;' )
  RETURN '\"' | s | '\"'
END
GO
CREATE FN [htm].[Encode]( s string ) RETURNS string AS
BEGIN
  SET s = REPLACE( s,'&', '&amp;' )
  SET s = REPLACE( s, '<', '&lt;' )
  RETURN s
END
GO
--############################################
CREATE SCHEMA [web]
CREATE TABLE [web].[File]([Path] string,[ContentType] string,[ContentLength] int,[Content] binary) 
GO
CREATE INDEX [ByPath] ON [web].[File]([Path])
GO
CREATE FN [web].[Cookie]( name string ) RETURNS string AS
BEGIN
  RETURN ARG( 3, name )
END
GO
CREATE FN [web].[Form]( name string ) RETURNS string AS
BEGIN
  RETURN ARG( 2, name )
END
GO
CREATE FN [web].[Head]( title string ) AS 
BEGIN 
  EXEC web.SetContentType( 'text/html;charset=utf-8' )
  SELECT '<html>
<head>
<meta http-equiv=\"Content-type\" content=\"text/html;charset=UTF-8\">
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>' | title | '</title>
<style>
   body{font-family:sans-serif;}
   body{ max-width:60em; }
</style>
</head>
<body>
<div style=\"color:white;background:lightblue;padding:4px;\">
<a href=/Menu>Menu</a> 
| <a target=_blank href=/Menu>New Window</a>
| <a href=Manual>Manual</a>
| <a target=_blank href=\"EditFunc?s=handler&n=' | web.Path() | '\">Code</a> ' | date.NowString() | ' UTC</div>'
END
GO
CREATE FN [web].[Main]() AS 
BEGIN 
  DECLARE path string SET path = web.Path()
  DECLARE ok string SET ok = Name FROM sys.Function WHERE Name = path AND Schema = 6
  IF ok = path
  BEGIN
    EXECUTE( 'EXEC ' | sys.Dot('handler',path) | '()' )
    DECLARE ex string
    SET ex = EXCEPTION()
    IF ex != ''
    BEGIN
      EXEC web.Head( 'Error' )
      SELECT '<h1>Error</h1><pre>'
      SELECT htm.Encode( ex )
      SELECT '</pre>'
      EXEC web.Trailer()
    END
  END
  ELSE
  BEGIN
    DECLARE ct string, content binary
    SET ok = Path, ct = ContentType, content = Content FROM web.File WHERE Path = path
    IF ok = path
    BEGIN
      EXEC web.SendBinary( ct, content )
    END    
    ELSE
    BEGIN
      EXEC web.Head( 'Unknown page')
      SELECT 'Unknown page Path=' | path
      EXEC web.Trailer()
    END
  END
END
GO
CREATE FN [web].[Path]() RETURNS string AS
BEGIN
  RETURN ARG(0,'')
END
GO
CREATE FN [web].[Query]( name string ) RETURNS string AS
BEGIN
  RETURN ARG( 1, name )
END
GO
CREATE FN [web].[Redirect]( url string ) AS
BEGIN
  DECLARE x int
  SET x = HEADER( 'location', url )
  SET x = STATUSCODE( 303 )
END
GO
CREATE FN [web].[SendBinary]( contenttype string, content binary ) AS
BEGIN
  EXEC web.SetContentType( contenttype )
  SELECT content
END
GO
CREATE FN [web].[SetContentType]( ct string ) AS
BEGIN
  DECLARE x int
  SET x = HEADER( 'contenttype', ct )
END
GO
CREATE FN [web].[SetCookie]( name string, value string, expires string ) AS
BEGIN
  /* Expires can be either in seconds e.g. Max-Age=1000000000
     or Expires=Wed, 09 Jun 2021 10:18:14 GMT
     or blank for temporary cookie

     To delete a cookie use e.g.

     EXEC web.SetCookie('username','','Max-Age=0')
  */
  DECLARE x int
  SET x = HEADER( 'set-cookie', name | '=' | value | '; ' | expires )
END
GO
CREATE FN [web].[Trailer]() AS
BEGIN
  SELECT '</body></html>'
END
GO
INSERT INTO [web].[File](Id,[Path],[ContentType],[ContentLength],[Content]) VALUES 
(7,'/favicon.ico','image/x-icon',1086,0x00000100010010100000010020002804000016000000280000001000000020000000010020000000000000000000000000000000000000000000000000000000ffbf0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffff0000ffff00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff000000000000000000000000000000000000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000000000000000000000000000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff00000000000000000000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff000000000000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000000000000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff00000000000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff000000000000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000000000000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff00000000000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff000000000000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffff0000ffbf)
GO

--############################################
CREATE SCHEMA [browse]
CREATE TABLE [browse].[Column]([Position] int,[Label] string,[Description] string,[RefersTo] int,[Default] string,[InputCols] int,[InputFunction] string,[InputRows] int,[Style] int,[DisplayFunction] string,[ParseFunction] string) 
GO
CREATE INDEX [ByRefersTo] ON [browse].[Column]([RefersTo])
GO
CREATE TABLE [browse].[Table]([NameFunction] string,[SelectFunction] string,[DefaultOrder] string,[Title] string,[Description] string,[Role] int) 
GO
CREATE FN [browse].[BrowseColumnName]( k int ) RETURNS string AS 
BEGIN
  SET result = sys.TableName( Table ) | '.' | sys.QuoteName( Name )
  FROM sys.Column WHERE Id = k
END
GO
CREATE FN [browse].[ChildSql]( colId int, k int ) RETURNS string AS 
BEGIN 
  /* Returns SQL to display a child table, with hyperlinks where a column refers to another table */
  DECLARE col string, colid int, colName string, type int, th string, ob string
  DECLARE table int SET table = Table FROM sys.Column WHERE Id = colId
  
  SET ob = DefaultOrder FROM browse.Table WHERE Id = table
  FOR colid = Id, type = Type,
    col = CASE WHEN Type % 8 = 2 THEN 'htm.Encode(' | Name | ')' ELSE Name END, colName = Name
  FROM sys.Column WHERE Table = table AND Id != colId
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, nf string, label string, df string
    SET ref = 0, nf = '', df = ''
    SET ref = RefersTo, label = Label, df = DisplayFunction FROM browse.Column WHERE Id = colid
    IF ref > 0 SET nf = NameFunction FROM browse.Table WHERE Id = ref
    SET ob = DefaultOrder FROM browse.Table WHERE Id = ref
    SET result |= '|''<TD' | CASE WHEN type % 8 != 2 THEN ' align=right' ELSE '' END | '>''|'
      | CASE 
        WHEN df != '' THEN df | '(' | col | ')'
        WHEN nf != '' 
        THEN '''<a href=\"/ShowRow?t=' | ref | '&k=''|' | col | '|''\">''|' | nf | '(' | col | ')' | '|''</a>''' 
        ELSE col
        END,
        th = th | '<TH>' | CASE WHEN label != '' THEN label ELSE colName END
  END
  DECLARE kcol string SET kcol = sys.QuoteName(Name) FROM sys.Column WHERE Id = colId
  RETURN 
   'SELECT ''<TABLE><TR><TH>' | th | ''' '
   | 'SELECT ' | '''<TR><TD><a href=\"ShowRow?t=' | table | '&k=''| Id | ''\">Show</a> '''
     | result | ' FROM ' | sys.TableName( table ) | ' WHERE ' | kcol | ' = ' | k | CASE WHEN ob != '' THEN ' ORDER BY ' | ob ELSE '' END
   | ' SELECT ''</TABLE>'''
END
GO
CREATE FN [browse].[ColNames]( table int ) RETURNS string AS
BEGIN
  DECLARE col string
  FOR col = '<a href=\"/BrowseColInfo?k=' | Id | '\">' | Name | '</a>' 
    | ' ' | sys.TypeName(Type) /* | ' pos=' | browse.ColPos(Id) */
  FROM sys.Column WHERE Table = table
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    SET result |= CASE WHEN result = '' THEN '' ELSE ', ' END | col
  END
END
GO
CREATE FN [browse].[ColParser]( colId int, type int, f string ) RETURNS string AS
BEGIN
  -- ColId not currently used, but in future user-specified parser could be fetched from Parse.Column
  DECLARE pf string
  SET pf = ParseFunction FROM browse.Column WHERE Id = colId
  RETURN CASE 
    WHEN pf != '' THEN pf | '(' | f | ')'
    WHEN type % 8 = 3 THEN 'PARSEINT(' | f |')'
    WHEN type % 8 = 4 THEN 'PARSEFLOAT(' | f | ')'
    WHEN type % 8 = 5 THEN 'browse.ParseBool(' | f | ')'
    ELSE f
  END
END
GO
CREATE FN [browse].[ColPos]( c int ) RETURNS int AS
BEGIN
  DECLARE pos int
  SET pos = Position FROM browse.Column WHERE Id = c
  RETURN pos
END
GO
CREATE FN [browse].[ColValues]( table int ) RETURNS string AS
BEGIN
  DECLARE col string, colid int
  FOR colid = Id, col = CASE 
    WHEN Type % 8 = 2 THEN 'htm.Encode(sys.SingleQuote(' | Name | '))'
    ELSE Name
  END
  FROM sys.Column WHERE Table = table 
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, nf string, df string
    SET ref = 0, nf = '', df = ''
    SET ref = RefersTo, df = DisplayFunction FROM browse.Column WHERE Id = colid
    IF ref > 0 SET nf = NameFunction FROM browse.Table WHERE Id = ref
    SET result |= CASE WHEN result = '' THEN '' ELSE '|'', ''|' END | 
      CASE 
      WHEN df != '' THEN df | '(' | col | ')'
      WHEN nf != '' 
      THEN '''<a href=\"/ShowRow?t=' | ref | '&k=''|' | col | '|''\">''|' | nf | '(' | col | ')' | '|''</a>''' 
      ELSE col
      END
  END
END
GO
CREATE FN [browse].[DefaultDefault]( type int, ref int ) RETURNS string AS
BEGIN
  RETURN CASE
    WHEN type % 8 = 2 THEN ''''''
    WHEN type % 8 = 1 THEN '0x'
    WHEN type % 8 = 5 THEN 'false'
    ELSE '0'
    END
END
GO
CREATE FN [browse].[DefaultInput]( type int ) RETURNS string AS
BEGIN
  RETURN CASE 
  WHEN type % 8 = 3 THEN 'browse.InputInt'
  WHEN type % 8 = 1 THEN 'browse.InputBinary'
  WHEN type % 8 = 4 THEN 'browse.InputDouble'
  WHEN type % 8 = 5 THEN 'browse.InputBool'
  ELSE 'browse.InputString'
  END
END
GO
CREATE FN [browse].[FormInsertSql]( table int, pc int ) RETURNS string AS
BEGIN
  DECLARE sql string, col string, type int, colId int
  FOR col = Name, type = Type, colId = Id FROM sys.Column 
    WHERE Table = table AND Id != pc
    ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, inf string, default string
    SET ref = 0, inf = '', default = ''
    SET ref = RefersTo,  inf = InputFunction, default = Default FROM browse.Column WHERE Id = colId
    IF ref > 0 AND inf = '' SET inf = SelectFunction FROM browse.Table WHERE Id = ref
    IF inf = '' SET inf = browse.DefaultInput( type )
    IF default = '' SET default = browse.DefaultDefault( type, ref )
 
    SET sql |= CASE WHEN sql = '' THEN '' ELSE ' | ' END
      | '''<p><label for=\"' | col | '\">' | col | '</label>: '' | ' 
      | inf | '(' | colId | ',' | default | ')'
  END
  RETURN 'SELECT ' | sql
END
GO
CREATE FN [browse].[FormUpdateSql]( table int, k int ) RETURNS string AS
BEGIN
  DECLARE sql string, col string, colId int, type int
  FOR col = Name, colId = Id, type = Type FROM sys.Column WHERE Table = table
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, inf string
    SET ref = 0, inf = ''
    SET ref = RefersTo, inf = InputFunction FROM browse.Column WHERE Id = colId
    IF ref > 0 AND inf = '' SET inf = SelectFunction FROM browse.Table WHERE Id = ref
    IF inf = '' SET inf = browse.DefaultInput( type )
    SET sql |= 
      CASE WHEN sql = '' THEN '' ELSE ' | ' END
      | '''<p><label for=\"' | col | '\">' | col | '</label>: '' | ' 
      | inf | '(' | colId | ',' | sys.QuoteName(col) | ')'
  END
  RETURN 'SELECT ' | sql | ' FROM ' | sys.TableName( table ) | ' WHERE Id =' | k
END
GO
CREATE FN [browse].[InputBinary]( colId int, value binary ) RETURNS string AS 
BEGIN 
  DECLARE cn string SET cn = Name FROM sys.Column WHERE Id = colId
  DECLARE size int SET size = InputCols FROM browse.Column WHERE Id = colId
  IF size = 0 SET size = 50
  RETURN '<input id=\"' | cn | '\" name=\"' | cn | '\" size=' | size | ' value=\"' | value | '\">'
END
GO
CREATE FN [browse].[InputBool]( colId int, value bool ) RETURNS string AS
BEGIN
  DECLARE cn string 
  SET cn = Name FROM sys.Column WHERE Id = colId
  RETURN '<input type=checkbox id=\"' | cn | '\" name=\"' | cn | '\"' | CASE WHEN value THEN ' checked' ELSE '' END | '>'
END
GO
CREATE FN [browse].[InputDouble]( colId int, value double ) RETURNS string AS 
BEGIN 
  DECLARE cn string SET cn = Name FROM sys.Column WHERE Id = colId
  DECLARE size int 
  SET size = InputCols FROM browse.Column WHERE Id = colId
  IF size = 0 SET size = 15
  RETURN '<input id=\"' | cn | '\" name=\"' | cn | '\" size=\"' | size | '\"' | ' value=\"' | value | '\">'
END
GO
CREATE FN [browse].[InputInt]( colId int, value int) RETURNS string AS 
BEGIN 
  DECLARE cn string 
  SET cn = Name FROM sys.Column WHERE Id = colId
  DECLARE size int
  SET size = InputCols FROM browse.Column WHERE Id = colId
  IF size = 0 SET size = 10
  RETURN '<input type=number id=\"' | cn | '\" name=\"' | cn | '\" size=' | size | ' value=' | value | '>'
END
GO
CREATE FN [browse].[InputString]( colId int, value string ) RETURNS string AS 
BEGIN 
  DECLARE cn string SET cn = Name FROM sys.Column WHERE Id = colId 
  DECLARE cols int, rows int, description string
  SET cols = InputCols, rows = InputRows, description = Description
  FROM browse.Column WHERE Id = colId
  IF cols = 0 SET cols = 50
  IF rows > 0
    RETURN '<textarea id=\"' | cn | '\" name=\"' | cn | '\" cols=\"' | cols | '\"' | '\" rows=\"' | rows |'\"'
      | CASE WHEN value = '' THEN 'placeholder=' | htm.Attr(description) ELSE '' END
      | '\">' | htm.Encode(value) | '</textarea>'
  ELSE
    RETURN '<input id=\"' | cn | '\" name=\"' | cn | '\" size=\"' | cols | '\"' | ' value=' | htm.Attr(value) | '>'
END
GO
CREATE FN [browse].[InputYearMonthDay]( colId int, value int) RETURNS string AS 
BEGIN 
  DECLARE cn string 
  SET cn = Name FROM sys.Column WHERE Id = colId
  DECLARE size int
  SET size = InputCols FROM browse.Column WHERE Id = colId
  IF size = 0 SET size = 10
  RETURN '<input id=\"' | cn | '\" name=\"' | cn | '\" size=' | size | ' value=' | htm.Attr(date.YearMonthDayToString(value)) | '>'
END
GO
CREATE FN [browse].[InsertNames]( table int ) RETURNS string AS
BEGIN
  DECLARE col string
  FOR col = Name FROM sys.Column WHERE Table = table
    SET result |= CASE WHEN result = '' THEN '' ELSE ',' END | sys.QuoteName(col)
  RETURN '(' | result | ')'
END
GO
CREATE FN [browse].[InsertSql]( table int, pc int, p int ) RETURNS string AS
BEGIN
  DECLARE vlist string, f string, type int, colId int
  FOR f = 'web.Form(' | sys.SingleQuote(Name) | ')', type = Type, colId = Id
  FROM sys.Column WHERE Table = table 
  SET vlist |= CASE WHEN vlist = '' THEN '' ELSE ' , ' END | 
    CASE 
    WHEN colId = pc THEN '' | p
    ELSE browse.ColParser( colId, type, f )
    END
  RETURN 'INSERT INTO ' | sys.TableName( table ) | browse.InsertNames( table ) | ' VALUES (' | vlist | ')'
END
GO
CREATE FN [browse].[ParseBool]( s string ) RETURNS bool AS
BEGIN
  RETURN s = 'on'
END
GO
CREATE FN [browse].[SchemaSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId
  DECLARE opt string, options string, sels string
  SET sels = web.Form( col )
  IF sels != '' SET sel = PARSEINT( sels )
  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | htm.Encode( Name ) | '</option>'
  FROM sys.Schema
  ORDER BY Name
  SET options |= opt
  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options | 
     '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
     | '</select>'
END
GO
CREATE FN [browse].[ShowSql]( table int, k int ) RETURNS string AS
BEGIN
  DECLARE cols string, col string, colname string, colid int
  FOR colid = Id, colname = Name, col = CASE 
    WHEN Type % 8 = 2 THEN 'htm.Encode(' | Name | ')'
    ELSE Name
    END
  FROM sys.Column WHERE Table = table 
  ORDER BY browse.ColPos(Id), Id
  BEGIN
    DECLARE ref int, nf string, df string
    SET ref = 0, nf = '', df = ''
    SET ref = RefersTo, df = DisplayFunction FROM browse.Column WHERE Id = colid
    IF ref > 0 SET nf = NameFunction FROM browse.Table WHERE Id = ref ELSE SET nf = ''
    SET cols |= 
      CASE WHEN cols = '' THEN '' ELSE ' | ' END
      | '''<p>' | colname | ': '' | '
      | CASE 
        WHEN df != '' THEN df | '(' | col | ')'
        WHEN nf != '' THEN '''<a href=\"/ShowRow?t=' | ref | '&k=''|' | col | '|''\">''|' | nf | '(' | col | ')' | '|''</a>''' 
        ELSE col
        END
  END
  DECLARE namefunc string SET namefunc = NameFunction FROM browse.Table WHERE Id = table
  RETURN '  
    DECLARE t int SET t = '|table|'
    DECLARE k int SET k = '|k|'
    DECLARE title string SET title = browse.TableTitle( t )' 
      | CASE WHEN namefunc = '' THEN '' ELSE ' | '' '' | ' | namefunc | '(k)' END | '
    EXEC web.Head( title )
    SELECT ''<b>'' | title | ''</b><br>''
  '
  | ' SELECT ' | cols | ' FROM ' | sys.TableName(table) | ' WHERE Id = k'
  | ' SELECT ''<p><a href=\"/EditRow?t='' | t | ''&k='' | k | ''\">Edit</a>'''
  | '
  DECLARE col int
  FOR col = Id FROM browse.Column WHERE RefersTo = t
  BEGIN
    SELECT ''<p><b>'' | browse.TableTitle( Table ) | ''</b>''
     | '' <a href=\"AddChild?c='' | col | ''&p='' | k | ''\">Add</a>''
    FROM sys.Column WHERE Id = col
    EXECUTE( browse.ChildSql( col, k ) )
  END
  SELECT ''<p><a href=\"/ShowTable?k='' | t | ''\">'' | browse.TableTitle(t) | '' Table</a>''
  EXEC web.Trailer()
'
END
GO
CREATE FN [browse].[TableSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId
  DECLARE opt string, options string
  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | htm.Encode( sys.TableName(Id) ) | '</option>'
  FROM sys.Table
  ORDER BY sys.TableName(Id)
  SET options |= opt
  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options | 
     '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
     | '</select>'
END
GO
CREATE FN [browse].[TableTitle]( table int ) RETURNS string AS
BEGIN
  SET result = Title FROM browse.Table WHERE Id = table
  IF result = '' SET result = Name FROM sys.Table WHERE Id = table
END
GO
CREATE FN [browse].[UpdateSql]( table int, k int ) RETURNS string AS
BEGIN
  DECLARE alist string, col string, type int, colId int
  FOR colId = Id, col = Name, type = Type FROM sys.Column WHERE Table = table
  BEGIN
    DECLARE f string SET f = 'web.Form(' | sys.SingleQuote(col) | ')'
    SET alist |= CASE WHEN alist = '' THEN '' ELSE ' , ' END
      | sys.QuoteName(col) | ' = ' | browse.ColParser( colId, type, f )
  END
  RETURN 'UPDATE ' | sys.TableName( table ) | ' SET ' | alist | ' WHERE Id =' | k
END
GO
--############################################
CREATE SCHEMA [handler]
CREATE FN [handler].[/AddChild]() AS
BEGIN
  DECLARE c int SET c = PARSEINT( web.Query('c') )
  DECLARE p int SET p = PARSEINT( web.Query('p') )
  DECLARE t int SET t = Table FROM sys.Column WHERE Id = c
  DECLARE ex string
  IF web.Form( '$submit' ) != '' 
  BEGIN
    EXECUTE( browse.InsertSql( t, c, p ) ) 
    SET ex = EXCEPTION()
    IF ex = '' 
    BEGIN
      EXEC web.Redirect( 'ShowRow?t=' | t | '&k=' | LASTID() )
      RETURN 
    END
  END
 
  DECLARE title string SET title = 'Add ' | browse.TableTitle( t )
  EXEC web.Head( title )
  SELECT '<b>' | title | '</b><br>'
  IF ex != '' SELECT '<p>Error: ' | ex
  SELECT '<form method=post>' 
  EXECUTE( browse.FormInsertSql( t, c ) )
  SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'
  EXEC web.Trailer()
    
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/AddRow]() AS 
BEGIN 
  DECLARE t int SET t = PARSEINT( web.Query('t') )
  DECLARE ex string
  IF web.Form( '$submit' ) != '' 
  BEGIN
    DECLARE lastid int
    SET lastid = LASTID()
    EXECUTE( browse.InsertSql( t, 0, 0 ) ) 
    SET ex = EXCEPTION()
    IF ex = '' 
    BEGIN
      EXEC web.Redirect( 'ShowRow?t=' | t | '&k=' | LASTID() )
      RETURN
    END
  END
  
  EXEC web.Head( 'Add ' | browse.TableTitle( t ) )
  IF ex != '' SELECT '<p>Error: ' | htm.Encode( ex )
  SELECT '<form method=post>' 
  EXECUTE( browse.FormInsertSql( t, 0 ) )
  SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/BrowseColInfo]() AS 
BEGIN 
  DECLARE tid int SET tid = 8
  DECLARE c int SET c = PARSEINT( web.Query( 'k' ) )
  DECLARE t int, colName string
  SET t = Table, colName = Name FROM sys.Column WHERE Id = c
  DECLARE ok int SET ok = 0
  SET ok = Id FROM browse.Column WHERE Id = c
  IF ok != c INSERT INTO browse.Column( Id ) VALUES ( c )
  IF web.Form( '$submit' ) != '' 
  BEGIN
    EXECUTE( browse.UpdateSql( tid, c ) ) 
    EXEC web.Redirect( 'ShowTable?k=' | t )
  END
  ELSE
  BEGIN
    EXEC web.Head( 'Column ' | colName )
    SELECT '<h1>Column ' | colName | '</h1><form method=post>' 
    EXECUTE( browse.FormUpdateSql( tid, c ) )
    SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'
    EXEC web.Trailer()
  END
END
GO
CREATE FN [handler].[/BrowseInfo]() AS 
BEGIN 
  DECLARE k int SET k = PARSEINT( web.Query( 'k' ) )
  DECLARE tid int SET tid = 9
  DECLARE ok int SET ok = 0
  SET ok = Id FROM browse.Table WHERE Id = k
  IF ok != k INSERT INTO browse.Table( Id ) VALUES ( k )
  IF web.Form( '$submit' ) != '' 
  BEGIN
    EXECUTE( browse.UpdateSql( tid, k ) ) 
    EXEC web.Redirect( 'ShowTable?k=' | k )
  END
  ELSE
  BEGIN
    EXEC web.Head( 'Browse Info for ' | sys.TableName(k) )
    SELECT '<form method=post>' 
    EXECUTE( browse.FormUpdateSql( tid, k ) )
    SELECT '<p><input name=\"$submit\" type=submit value=Save></form>'
    EXEC web.Trailer()
  END
END
GO
CREATE FN [handler].[/CheckAll]() AS 
BEGIN
  EXEC web.Head('Check All Functions compile')
  DECLARE sid int, sname string, fname string, err int, n int

  FOR sid = Id, sname = sys.QuoteName(Name) FROM sys.Schema
  BEGIN
    FOR fname = sys.QuoteName(Name) FROM sys.Function WHERE Schema = sid
    BEGIN
      -- SELECT '<br>Checking ' | sname | '.' | fname
      EXECUTE( 'CHECK ' | sname | '.' | fname )
      DECLARE ex string SET ex = EXCEPTION()
      IF ex != '' 
      BEGIN
        SELECT '<br>Error : ' | htm.Encode(ex)
        SET err = err + 1
      END
      SET n = n + 1
    END
  END
  SELECT '<p>' | n | ' functions checked, errors=' | err | '.'
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/EditFile]() AS
BEGIN
  DECLARE k int SET k = PARSEINT( web.Query('k') )
  DECLARE path string SET path = web.Form('path')
  IF path != '' 
  BEGIN
    UPDATE web.File SET Path = path WHERE Id = k
    EXEC web.Redirect('ListFile')
  END
  ELSE
  BEGIN
    EXEC web.Head( 'Edit File' )
    SELECT '<h1>Edit File Path</h1>'
    SELECT '<form method=post>Path: <input name=path size=50 value=\"' | Path | '\">'
      | '<p><input type=submit value=Save></form>'
    FROM web.File WHERE Id = k
    EXEC web.Trailer()
  END
END
GO
CREATE FN [handler].[/EditFunc]() AS
BEGIN
  DECLARE s string SET s = web.Query('s')
  DECLARE n string SET n = web.Query('n')
  DECLARE sid int SET sid = Id FROM sys.Schema WHERE Name = s
  DECLARE def string, ex string SET def = web.Form('def')
  IF def != '' 
  BEGIN
    EXECUTE( 'ALTER FN ' | sys.Dot(s,n) | def )
    SET ex = EXCEPTION()
  END
  ELSE SET def = Def FROM sys.Function WHERE Schema = sid AND Name = n 
  EXEC web.Head( 'Edit ' | n )
  IF ex != '' SELECT '<p>Error: ' | htm.Encode( ex )
  SELECT 
     '<p><form method=post>'
     | '<input type=submit value=\"ALTER\"> <a href=ShowSchema?s=' | s | '>' | s | '</a> . ' | n 
     | CASE WHEN s = 'handler' THEN ' <a href=' | n | '>Go</a>' ELSE '' END
     | '<br><textarea name=def rows=40 cols=150>' | htm.Encode(def) | '</textarea>' 
     | '</form>' 
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/EditRow]() AS 
BEGIN 
  DECLARE t int SET t = PARSEINT( web.Query('t') )
  DECLARE k int SET k = PARSEINT( web.Query('k') )
  DECLARE ex string
  DECLARE submit string SET submit = web.Form( '$submit' )
  IF submit != '' 
  BEGIN
    IF submit = 'Save'
    BEGIN
      EXECUTE( browse.UpdateSql( t, k ) ) 
      SET ex = EXCEPTION()
      IF ex = '' 
      BEGIN
        EXEC web.Redirect( 'ShowRow?t=' | t | '&k=' | k )
        RETURN
      END
    END
    ELSE IF submit = 'Delete'
    BEGIN
      EXECUTE( 'DELETE FROM ' | sys.TableName( t ) | ' WHERE Id =' | k )
      EXEC web.Redirect( 'Menu' )
      RETURN
    END      
  END
 
  EXEC web.Head( 'Edit ' | browse.TableTitle( t ) )
  IF ex != '' SELECT '<p>Error: ' | htm.Encode(ex)
  SELECT '<form method=post>' 
  
  EXECUTE( browse.FormUpdateSql( t, k ) )
  SELECT '<p><input name=\"$submit\" type=submit value=Save> <input name=\"$submit\" type=submit value=Delete></form>'
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/Execute]() AS 
BEGIN
  DECLARE sql string SET sql = web.Form('sql')
  EXEC web.Head( 'Execute' )
  SELECT 
     '<p><form method=post>'
     | 'SQL to <input type=submit value=Execute>'
     | '<br><textarea name=sql rows=20 cols=100' | CASE WHEN sql='' THEN ' placeholder=\"Enter SQL here. See Manual for details.\"' ELSE '' END | '>' | htm.Encode(sql) | '</textarea>' 
     | '</form>' 
  IF sql != '' 
  BEGIN
    -- EXEC SETMODE( 1 ) -- Causes result tables to be displayed as HTML tables
    EXECUTE( sql ) 
    -- EXEC SETMODE( 0 )
    DECLARE ex string SET ex = EXCEPTION()
    IF ex != '' SELECT '<p>Error : ' | htm.Encode(ex)
  END
  SELECT '<p>Example SQL:'
     | '<br>SELECT dbo.CustName(Id) AS Name, Age FROM dbo.Cust'
     | '<br>SELECT Cust, Total FROM dbo.Order'
     | '<br>EXEC date.Test( 2020, 1, 1, 60 )'
     | '<br>CREATE TABLE dbo.Cust( LastName string, Age int )'
     | '<br>CREATE INDEX ByLastName ON dbo.Cust(LastName)'
     | '<br>CREATE FN handler.[/MyPage]() AS BEGIN END'
     | '<br>SELECT ''hash='' | ARGON( ''argon2i!'', ''delicious salt'' )'
     | '<br>EXEC web.SetCookie(''username'',''fred'',''Max-Age=1000000000'')'
     | '<br>EXEC rtest.OneTest()'
     | '<br>DROP INDEX ByLastName ON dbo.Cust'  
     | '<br>ALTER TABLE dbo.Cust MODIFY FirstName string(20), ADD [City] string, PostCode string'
     | '<br>ALTER TABLE dbo.Cust DROP PostCode'
     | '<br>DROP TABLE dbo.Cust'
   EXEC web.Trailer()
END
GO
CREATE FN [handler].[/FileUpload]() AS
BEGIN
  EXEC web.Head( 'File upload' )
  IF FILEATTR(0,0) = 'file' 
  BEGIN
    SELECT '<p>Filename=' | FILEATTR(0,2) | ' ContentType=' | FILEATTR(0,1)
    DECLARE content binary SET content =  FILECONTENT(0)
    
    INSERT INTO web.File( Path, ContentType, ContentLength, Content )
    VALUES ( '/Uploads/' | FILEATTR(0,2), FILEATTR(0,1), BINLEN(content), content )
  END
  SELECT '<form method=post enctype=\"multipart/form-data\"><p><Input name=file type=file><p><input name=submit type=submit value=Upload></form>'
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/ListFile]() AS
BEGIN
  EXEC web.Head( 'Files' )
  SELECT '<h1>Files</h1>' 
  SELECT '<p>Path=<a target=_blank href=\"' | Path | '\">' | Path | '</a> Type= ' | ContentType 
   | ' Length=' | ContentLength | ' id=' | Id | ' <a href=\"/EditFile?k=' | Id | '\">Edit Path</a>'
  FROM web.File
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/Manual]() AS BEGIN
EXEC web.Head('Manual')
SELECT '<h1>Manual</h1>
<p>This manual describes the various SQL statements that are available. Where syntax is described, optional elements are enclosed in square brackets.
<h2>Schema definition</h2>
<h3>CREATE SCHEMA</h3>
<p>CREATE SCHEMA name
<p>Creates a new schema. Every database object (Table, Function) has an associated schema. Schemas are used to organise database objects into logical categories.
<h2>Table definition</h2>
<h3>CREATE TABLE</h3><p>CREATE TABLE schema.tablename ( Colname1 Coltype1, Colname2 Coltype2, ... )
<p>Creates a new base table. Every base table is automatically given an Id column, which auto-increments on INSERT ( if no explicit value is supplied).<p>The data types are as follows:
<ul>
<li>int(n), 1 <= n <= 8. Signed n-byte integer. Default is 8 bytes.</li>
<li>float, double : floating point numbers of size 4 and 8 bytes respectively.</li>
<li>string(n) : a variable length string of unicode characters. n (optional, default 15) specifies number of bytes stored inline.</li>
<li>binary(n) : a variable length string of bytes. n (optional, default 15) specifies number of bytes stored inline.</li>
<li>bool : boolean ( true or false ).</li>
</ul>

<p>Each data type has a default value : zero for numbers, a zero length string for string and binary, and false for the boolean type. The variable length data types are stored in a special system table if the length exceeds the reserved inline storage, meaning they are slightly slower to store and retrieve. Local float and integer variables and arithmetic operations are all 64 bits (8 bytes). The lower precision only applies when a value is stored in column of a table.
<h3>ALTER TABLE</h3>
<p>ALTER TABLE schema.tablename action1, action2 .... <p>The actions are as follows:
<ul>
<li>ADD Colname Coltype : a new column is added to the table.</li>
<li>MODIFY Colname Coltype : the datatype of an existing column is changed. The only changes allowed are between the different sizes of integers, between float and double, and modification of the number of bytes stored inline for binary and string.</li>
<li>DROP Colname : the column is removed from the table.</li>
</ul>
<p>Note: currently, any indexes that have been added to a table need to be dropped before using ALTER TABLE. They can be added again afterwards.
</ul>
<h2>Data manipulation statements</h2>
<h3>INSERT</h3>
<p>INSERT INTO schema.tablename ( Colname1, Colname2 ... ) VALUES ( Val1, Val2... ) [,] ( Val3, Val4 ...) ...
<p>The specified values are inserted into the table. The values may be any expressions ( possibly involving local variables or function calls ).
<h3>SELECT</h3><p>SELECT expressions FROM source-table [WHERE bool-exp ] [ORDER BY expressions]
<p>A new table is computed, based on the list of expressions and the WHERE and ORDER BY clauses.
<p>If the keyword DESC is placed after an ORDER BY expression, the order is reversed ( descending order ).
<p>The SELECT expressions can be given names using AS.
<p>When used as a stand-alone statement, the results are passed to the code that invoked the batch, and may be displayed to a user or sent to a client for further processing and eventual display. 
<h3>UPDATE</h3><p>UPDATE schema.tablename SET Colname1 = Exp1, Colname2 = Exp2 .... WHERE bool-exp
<p>Rows in the table which satisfy the WHERE condition are updated.
<h3>DELETE</h3><p>DELETE FROM schema.tablename WHERE bool-exp
<p>Rows in the table which satisfy the WHERE condition are removed.
<h2>Local variable declaration and assignment statements</h2>
<h3>DECLARE</h3><p>DECLARE name1 type1, name2 type2 ....
<p>Local variables are declared with the specified types. The variables are initialised to default values ( but only once, not each time the DECLARE is encountered if there is a loop ).
<h3>SET</h3>
<p>SET name1 = exp1, name2 = exp2 .... [ FROM table ] [ WHERE bool-exp ]
<p>Local variables are assigned. If the FROM clause is specified, the values are taken from a table row which satisfies the WHERE condition. If there is no such row, the values of the local variables remain unchanged.
<h3>FOR</h3><p>FOR name1 = exp1, name2 = exp2 .... FROM table [ WHERE bool-exp ] [ORDER BY expressions] Statement
<p>Statement is repeatedly executed for each row from the table which satisfies the WHERE condition, with the named local variables being assigned expressions which depend on the rows.
<h2>Control flow statements</h2>
<h3>BEGIN .. END</h3><p>BEGIN Statement1 Statement2 ... END
<p>The statements are executed in order. A BEGIN..END compound statement can be used whenever a single statement is allowed.
<h3>IF .. THEN ... ELSE ...</h3>
<p>IF bool-exp THEN Statement1 [ ELSE Statement2 ]
<p>If bool-exp evaluates to true Statement1 is executed, otherwise Statement2 ( if specified ) is executed.
<h3>WHILE</h3><p>WHILE bool-exp Statement
<p>Statement is repeatedly executed as long as bool-exp evaluates to true. See also BREAK.
<h3>GOTO</h3><p>GOTO label
<p>Control is transferred to the labelled statement. A label consists of a name followed by a colon (:)
<h3>BREAK</h3><p>BREAK
<p>Execution of the enclosing FOR or WHILE loop is terminated.
<h2>Batch execution</h2><p>EXECUTE ( string-expression )
<p>Evaluates the string expression, and then executes the result ( which should be a list of SQL statements ).
<p>Note that database objects ( tables, function ) must be created in a prior batch before being used. A GO statement may be used to signify the start of a new batch.
<h2>Stored Functions</h2>
<h3>CREATE FN</h3><p>CREATE FN schema.name ( param1 type1, param2 type2... ) AS BEGIN statements END
<p>A stored function ( no return value ) is created, which can later be called by an EXEC statement.
<h3>EXEC</h3><p>EXEC schema.name( exp1, exp2 ... )
<p>The stored function is called with the supplied parameters.
<h3>Exceptions</h3><p>An exception will terminate the execution of a function or batch. EXCEPTION() can be used to obtain a string describing the most recent exception (and clears the exception string). If any exception occurs, the database is left unchanged.
<h3>THROW</h3>
<p>THROW string-expression 
<p>An exception is raised, with the error message being set to the string.
<h3>CREATE FN</h3><p>CREATE FN schema.name ( param1 type1, param2 type2... ) RETURNS type AS BEGIN statements END
<p>A stored function is created which can later be used in expressions.
<h3>RETURN</h3>
<p>RETURN expression
<p>Returns a value from a stored function. RETURN with no expression returns from a stored function with no return value.
<p>The pre-defined local variable result can be assigned instead to set the return value.
<h2>Expressions</h2>
<p>Expressions are composed from literals, named local variables, local parameters and named columns from tables. These may be combined using operators, stored functions, pre-defined functions. There is also the CASE expression, which has syntax CASE WHEN bool1 THEN exp1 WHEN bool2 THEN exp2 .... ELSE exp END - the result is the expression associated with the first bool expression which evaluates to true.
<h3>Literals</h3>
<p>String literals are written enclosed in single quotes. If a single quote is needed in a string literal, it is written as two single quotes. Binary literals are written in hexadecimal preceded by 0x. Integers are a list of digits (0-9). The bool literals are true and false.
<h3>Names</h3><p>Names are enclosed in square brackets and are case sensitive ( although language keywords such as CREATE SELECT are case insensitive, and are written without the square brackets, often in upper case only by convention ). The square brackets can be omitted if the name consists of only letters (A-Z,a-z).
<h3>Operators</h3>
<p>The operators ( all binary, except for - which can be unary, and NOT which is only unary ) in order of precedence, high to low, are as follows:
<ul>
<li>*  / % : multiplication, division and remainder (after division) of numbers. Remainder only applies to integers.</li>
<li>+ - : addition, subtraction of numbers.</li>
<li>| : concatenation of strings. The second expression is automatically converted to string if necessary.</li>
<li>= != > < >= <= : comparison of any data type.</li>
<li>NOT : boolean negation ( result is true if arg is false, false if arg is true ).</li>
<li>AND : boolean operator ( result is true if both args are true )</li>
<li>OR : boolean operator  ( result is true if either arg is true )</li>
</ul>
<p>Brackets can be used where necessary, for example ( a + b ) * c.
<h3>Pre-defined functions</h3>
<ul>
<li>LEN( s string ) : returns the length of s, which must be a string expression.</li>
<li>SUBSTRING( s string, start int, len int ) : returns the substring of s from start (1-based) length len.</li>
<li>REPLACE( s string, pat string, sub string ) : returns a copy of s where every occurrence of pat is replaced with sub.</li>
<li>LASTID() : returns the last Id value allocated by an INSERT statement.</li>
<li>PARSEINT( s string ) : parses an integer from s.</li>
<li>PARSEFLOAT( s string ) : parses a floating point number from s.</li>
<li>EXCEPTION() returns a string with any error that occurred during an EXECUTE statement.</li>
<li>REPACKFILE(k,schema,table) : A file is re-packed to free up pages. k=0 => main file, k=1.. => an index, k in -4..-1 => byte storage files. 
<li>See the web schema for functions that can be used to access http requests.</li>
</ul>
<h3>Conversions</h3>
<p>To be decided. Currently the only implicit conversion is to string for operands of string concatenation.
<h2>Indexes
<h3>CREATE INDEX</h3><p>CREATE INDEX indexname ON schema.tablename( Colname1, Colname2 ... )<p>Creates a new index. Indexes allow efficient access to rows other than by Id values. 
<p>For example, <br>CREATE INDEX ByCust ON dbo.Order(Cust) 
<br>creates an index allowing the orders associated with a particular customer to be efficiently retrieved without scanning the entire order table.
<h2>Drop</h2>
<h3>DROP object-type object-name</h3><p>object-type can be any one of SCHEMA,TABLE or FUNCTION.
<p>The specified object is removed from the database. In the case of a SCHEMA, all objects in the SCHEMA are also removed. In the case of TABLE, all the rows in the table are also removed.
<h3>DROP INDEX</h3><p>DROP INDEX indexname ON schema.tablename<p>The specified index is removed from the database.
<h2>Comments</h2>
<p>There are two kinds of comments. Single line comments start with -- and extend to the end of the line. Delimited comments start with /* and are terminated by */. Comments have no effect, they are simply to help document the code.
<h2>Comparison with other SQL implementations</h2><p>There is a single variable length string datatype \"string\" for unicode strings ( equivalent to nvarchar(max) in MSSQL ), no fixed length strings.
<p>Similarly there is a single binary datatype \"binary\" equivalent to varbinary(max) in MSSQL.
<p>Every table automatically gets an integer Id field ( it does not have to be specified ), which is automatically filled in if not specified in an INSERT statement. Id values must be unique ( an attempt to insert or assign a duplicate Id will raise an exception ). 
<p>WHERE condition is not optional in UPDATE and DELETE statements - WHERE true can be used if you really want to UPDATE or DELETE all rows. This is a \"safety\" feature.
<p>Local variables cannot be assigned with SELECT, instead SET or FOR is used, can be FROM a table, e.g.
<p>DECLARE s string SET s = Name FROM sys.Schema WHERE Id = schema
<p>No cursors ( use FOR instead ).
<p>Local variables cannot be assigned in a DECLARE statement.
<p>No default schemas. Schema of tables and functions must always be stated explicitly.
<p>No nulls. Columns are initialised to default a value if not specified by INSERT, or when new columns are added to a table by ALTER TABLE.
<p>No triggers. No joins. No outer references.
<h2>Guide to the pre-defined schemas</h2>
<h3>sys</h3>
<p>Has core system tables for language objects and related functions.
<h3>web</h3>
<p>Has the function that handles web requests ( web.main ) and other functions related to handling web requests.
<h3>handler</h3>
<p>Has handler functions, one for each web page.
<h3>htm</h3>
<p>Has functions related to encoding html.
<h3>browse</h3><p>Has tables and functions for displaying, editing arbitrary tables in the database.
<h3>date</h3><p>Has functions for manipulating dates - conversions between Days ( from year 0 ), Year-Day, Year-Month-Day and string.
' 
EXEC web.Trailer()
END
GO
CREATE FN [handler].[/Menu]() AS
BEGIN
   EXEC web.Head('Menu')
   SELECT '
<p><a href=\"/ShowTable?k=10\">Customers</a> | <a href=\"/OrderSummary\">Order Summary</a>
<h1>System</h1>
<p><a href=/Execute>Execute SQL</a>
<p><a href=/ListFile>Files</a>
<p><a href=/FileUpload>File Upload</a>
<p><a target=_blank href=/ScriptAll>Script entire database</a>
<p><a href=/CheckAll>Check all functions compile ok</a>
<p><a href=/VerifyDb>Verify database</a>
<h1>Schemas</h1>'
   SELECT '<p><a href=ShowSchema?s=' | Name | '>' | Name | '</a>' FROM sys.Schema ORDER BY Name
   EXEC web.Trailer()
END
GO
CREATE FN [handler].[/OrderSummary]() AS
BEGIN
  EXEC web.Head( 'Order Summary' )
  SELECT '<table><tr><th>Cust<th>Total<th>Count</tr>'

  DECLARE cust int, total int, sum int, count int, gsum int, gcount int
  FOR cust = Id FROM dbo.Cust ORDER BY FirstName, LastName
  BEGIN
    SET sum = 0, count = 0
    FOR total = Total FROM dbo.Order WHERE Cust = cust 
      SET sum = sum + total, count = count + 1
    SELECT '<tr><td><a href=ShowRow?t=10&k=' | cust | '>' | dbo.CustName(cust) | '</a>' 
      | '<td align=right>' | sum | '<td align=right>' | count
    SET gsum = gsum + sum, gcount = gcount + count
  END
  SELECT '</table>'
  SELECT '<p>Grand total =' | gsum | ' count=' | gcount
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/Rtest]() AS 
BEGIN 
  -- Can be invoked repeatedly with e.g. for /l %x in (1, 1, 100) do curl -X POST http://localhost:3000/Rtest
  -- Depending on the server, a POST request is needed, as GET requests maybe be assumed to be read-only.
  EXEC rtest.OneTest() 
END
GO
CREATE FN [handler].[/ScriptAll]() AS 
BEGIN 
  EXEC web.SetContentType( 'text/plain;charset=utf-8' )
  DECLARE s int
  FOR s = Id FROM sys.Schema
    EXEC sys.ScriptSchema(s)
  FOR s = Id FROM sys.Schema
    EXEC sys.ScriptSchemaBrowse(s)
END
GO
CREATE FN [handler].[/ShowRow]() AS 
BEGIN
  DECLARE t int SET t = PARSEINT( web.Query('t') )
  DECLARE k int SET k = PARSEINT( web.Query('k') )
  EXECUTE( browse.ShowSql( t, k ) )
END
GO
CREATE FN [handler].[/ShowSchema]() AS
BEGIN
  DECLARE s string SET s = web.Query('s')
  DECLARE sid int SET sid = Id FROM sys.Schema WHERE Name = s
  EXEC web.Head( 'Schema ' | s )
  SELECT '<h1>Schema ' | s | '</h1>'
  SELECT '<h2>Tables</h2>'
  SELECT '<p><a href=\"ShowTable?k=' | Id | '\">' | Name | '</a>'
  FROM sys.Table WHERE Schema = sid ORDER BY Name
  SELECT '<h2>Functions</h2>' 
  SELECT '<p><a href=\"EditFunc?s=' | s | '&n=' | Name | '\">' | Name | '</a>'
  FROM sys.Function WHERE Schema = sid ORDER BY Name
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/ShowTable]() AS 
BEGIN 
  DECLARE t int SET t = PARSEINT( web.Query('k') )
  DECLARE title string SET title = browse.TableTitle( t )
  SET title = title | ' Table'
  EXEC web.Head( title )
  SELECT '<b>' | title | '</b> <a href=/BrowseInfo?k=' | t | '>Settings</a>'   
    | '<p><b>Columns:</b> ' | browse.ColNames( t )
/*
  SELECT '<p><b>Indexes</b>'
  SELECT '<br>' | sys.QuoteName(Name) | ' ' | sys.IndexCols(Id)
  FROM sys.Index WHERE Table = t
*/
  SELECT '<p><b>Rows</b> <a href=\"AddRow?t=' | t | '\">Add</a>'
  
  DECLARE orderBy string SET orderBy = DefaultOrder FROM browse.Table WHERE Id = t
  DECLARE sql string SET sql ='SELECT ''<br><a href=\"ShowRow?t=' | t | '&k=''| Id |''\">Show</a> ''| ''''|' 
    | browse.ColValues(Id)  
    | ' FROM ' 
    | sys.TableName(Id)
    | CASE WHEN orderBy != '' THEN ' ORDER BY ' | orderBy ELSE '' END
  FROM sys.Table WHERE Id = t
  EXECUTE( sql )
  EXEC web.Trailer()
END
GO
CREATE FN [handler].[/VerifyDb]() AS
BEGIN
  EXEC web.Head('Verify Database')
  SELECT VERIFYDB()
  EXEC web.Trailer()
END
GO
--############################################
CREATE SCHEMA [dbo]
CREATE TABLE [dbo].[Cust]([FirstName] string(10),[LastName] string,[Age] int,[Postcode] string(10),[City] string) 
GO
CREATE TABLE [dbo].[Order]([Cust] int,[Total] int,[Date] int,[info] string(200)) 
GO
CREATE INDEX [ByCust] ON [dbo].[Order]([Cust])
GO
CREATE FN [dbo].[CustName]( cust int ) RETURNS string AS
BEGIN
  SET result = 'Cust ' | cust -- default in case Cust row does not exist
  SET result = FirstName | ' ' | LastName FROM dbo.Cust WHERE Id = cust
END
GO
CREATE FN [dbo].[CustSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId

  DECLARE opt string, options string

  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | htm.Encode( dbo.CustName(Id) ) | '</option>'
  FROM dbo.Cust
  ORDER BY LastName, FirstName
  SET options |= opt

  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options 
    | '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
    | '</select>'
END
GO
CREATE FN [dbo].[MakeOrders]() AS
BEGIN 
  DELETE FROM dbo.Order WHERE 1 = 1
  DECLARE date int SET date = date.DaysToYearMonthDay(date.Today())
  DECLARE @I int 
  SET @I=0 
  WHILE @I < 1000 -- Use 5000000 to stress system a bit!
  BEGIN 
    INSERT INTO dbo.[Order](Cust,Total,Date) VALUES(1+@I%7, ( 501 * (@I%11+@I%7) ) / 100, date ) 
    SET @I=@I+1 
  END
END
GO
INSERT INTO [dbo].[Cust](Id,[FirstName],[LastName],[Age],[Postcode],[City]) VALUES 
(1,'Mary','Poppins',65,'EC4 2NX','')
(2,'Clare','Smith',31,'GL3','')
(3,'Ron','Jones',45,'','')
(4,'Peter','Perfect',36,'','')
(5,'George','Washington',31,'WC1','')
(6,'Ron','Williams',49,'','')
(7,'Ben','Johnson',0,'','')
(8,'Alex','Barwood',63,'GL2 4LZ','')
GO

INSERT INTO [dbo].[Order](Id,[Cust],[Total],[Date],[info]) VALUES 
GO

--############################################
CREATE SCHEMA [email]
CREATE TABLE [email].[Msg]([from] string,[to] string,[title] string,[body] string,[format] int(1),[status] int(1)) 
GO
CREATE TABLE [email].[Queue]([msg] int,[sendtime] int,[retry] int) 
GO
CREATE INDEX [BySendTime] ON [email].[Queue]([sendtime])
GO
CREATE FN [email].[MsgName](id int) RETURNS string AS
BEGIN
  SET result = '' | id
END
GO
CREATE FN [email].[MsgSelect]( colId int, sel int ) RETURNS string AS
BEGIN
  DECLARE col string SET col = Name FROM sys.Column WHERE Id = colId

  DECLARE opt string, options string

  FOR opt = '<option ' | CASE WHEN Id = sel THEN ' selected' ELSE '' END 
  | ' value=' | Id | '>' | htm.Encode( email.MsgName(Id) ) | '</option>'
  FROM email.Msg
  ORDER BY Id
  SET options |= opt

  RETURN '<select id=\"' | col | '\" name=\"' | col | '\">' | options 
    | '<option ' | CASE WHEN sel = 0 THEN ' selected' ELSE '' END | ' value=0></option>'
    | '</select>'
END
GO
CREATE FN [email].[Sent](id int) AS
BEGIN
  DELETE FROM email.Queue WHERE msg = id
END
GO
INSERT INTO [email].[Msg](Id,[from],[to],[title],[body],[format],[status]) VALUES 
(1,'george.barwood@gmail.com','george.barwood@outlook.com','Test','Hello there George!',1,0)
GO

INSERT INTO [email].[Queue](Id,[msg],[sendtime],[retry]) VALUES 
(4,1,63774774310551966,0)
GO

--############################################
CREATE SCHEMA [rtest]
CREATE TABLE [rtest].[Gen]([x] int) 
GO
CREATE FN [rtest].[OneTest]() AS
BEGIN 
  DECLARE rtestdata int
  SET rtestdata = Id FROM sys.Schema WHERE Name = 'rtestdata'

  DECLARE r int
  SET r = x FROM rtest.Gen
  SET r = r * 48271 % 2147483647
  UPDATE rtest.Gen SET x = r WHERE true

  DECLARE sql string, a int
  SET a = r % 2

  DECLARE tname string
  SET tname = 't' | ( r / 100 ) % 7

  DECLARE exists string
  SET exists = ''
  SET exists = Name FROM sys.Table WHERE Schema = rtestdata AND Name = tname

  SET sql = CASE 
    WHEN exists = '' THEN 
      CASE WHEN r % 2 =1 THEN 'CREATE TABLE rtestdata.[' | tname | '](x string, y int(5))'
      ELSE 'CREATE TABLE rtestdata.[' | tname | '](x string, y int(3), z string )'
      END
    WHEN r % 10 = 0 THEN 'DROP TABLE rtestdata.[' | tname | ']'
    WHEN r % 2 = 1 THEN 'INSERT INTO rtestdata.[' | tname | '](x,y) VALUES ( rtest.repeat(''George'','|(r % 100)|'),' | (r % 10) | ')'
    ELSE 'DELETE FROM rtestdata.[' | tname | '] WHERE y = ' | ( r%15)
  END
  
  SELECT 'sql=' | sql

  EXECUTE( sql )
 
END
GO
CREATE FN [rtest].[repeat]( s string, n int ) RETURNS string AS
BEGIN
  WHILE n > 0
  BEGIN
    SET result |= s
    SET n = n - 1
  END
END
GO
INSERT INTO [rtest].[Gen](Id,[x]) VALUES 
(1,2061969400)
GO

DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Column'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Table'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',2,'',0,'',0,0,'','')
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Type'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',0,'',0,'',0,0,'sys.TypeName','')
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Function'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Schema'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',1,'',0,'',0,0,'','')
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Index'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'sys.IndexName','','','','',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Table'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',2,'',0,'',0,0,'','')
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'IndexColumn'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Index'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',4,'',0,'',0,0,'','')
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Schema'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'sys.SchemaName','browse.SchemaSelect','','','',0)
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'sys'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Table'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'sys.TableName','browse.TableSelect','','','',0)
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Schema'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',1,'',0,'',0,0,'','')
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'web'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'File'
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'browse'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Column'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'RefersTo'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',2,'',0,'',0,0,'','')
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'browse'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Table'
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'dbo'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Cust'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'dbo.CustName','dbo.CustSelect','','Customer','',0)
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'dbo'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Order'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Cust'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',10,'',0,'',0,0,'','')
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'Date'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',0,'date.DaysToYearMonthDay(date.Today())',0,'browse.InputYearMonthDay',0,0,'date.YearMonthDayToString','date.StringToYearMonthDay')
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'email'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Msg'
INSERT INTO browse.Table(Id,NameFunction, SelectFunction, DefaultOrder, Title, Description, Role) 
VALUES (tid,'email.MsgName','email.MsgSelect','','','',0)
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'email'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Queue'
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'msg'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',12,'',0,'',0,0,'','')
SET cid=Id FROM sys.Column WHERE Table = tid AND Name = 'sendtime'
INSERT INTO browse.Column(Id,[Position],[Label],[Description],[RefersTo],[Default],[InputCols],[InputFunction],[InputRows],[Style],[DisplayFunction],[ParseFunction]) 
VALUES (cid, 0,'','',0,'date.Ticks()',0,'',0,0,'date.MicroSecToString','')
GO
DECLARE tid int, sid int, cid int
SET sid = Id FROM sys.Schema WHERE Name = 'rtest'
SET tid = Id FROM sys.Table WHERE Schema = sid AND Name = 'Gen'
GO";