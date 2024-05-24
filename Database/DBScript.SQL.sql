/* Rebuild
DROP TABLE Options, Keys;
*/

IF NOT EXISTS ( SELECT * FROM sys.tables WHERE name = 'Keys' )
CREATE TABLE Keys (
	KeyID int IDENTITY(1,1) NOT NULL PRIMARY KEY,
	[Key] nvarchar(100) NOT NULL UNIQUE
)

IF NOT EXISTS ( SELECT * FROM sys.tables WHERE name = 'Options' )
CREATE TABLE Options (
	KeyID int NOT NULL CONSTRAINT FK_Options_KeyID REFERENCES Keys,
	[Option] nvarchar(100) NOT NULL,
	PRIMARY KEY (KeyID, "Option")
)
GO

/* Tests
INSERT INTO Keys VALUES ('One')
INSERT INTO Options VALUES (1, 'Option 1')
SELECT COUNT(*) FROM Keys
SELECT COUNT(*) FROM Options
EXEC sp_ClearTables
*/
CREATE OR ALTER PROCEDURE sp_ClearTables AS
DECLARE @SQL VARCHAR(2000)
SET @SQL='
ALTER TABLE Options DROP CONSTRAINT FK_Options_KeyID
TRUNCATE TABLE Options
TRUNCATE TABLE Keys
ALTER TABLE Options ADD CONSTRAINT FK_Options_KeyID FOREIGN KEY (KeyID) REFERENCES Keys'
EXEC (@SQL)
GO

CREATE OR ALTER VIEW v_Options AS
SELECT k.*, o.[Option]
FROM Keys k
JOIN Options o ON (o.KeyID = k.KeyID)
GO

/* Tests
SELECT COUNT(*)/1000000 FROM Keys
SELECT TOP 100 * FROM Keys ORDER BY [Key]
DECLARE @KeyID int EXEC @KeyID = sp_GetKeyID 'key1' SELECT @KeyID AS KeyID
*/
CREATE OR ALTER PROCEDURE sp_GetKeyID @Key nvarchar(100), @KeyID int = NULL OUTPUT AS
BEGIN
	SELECT @KeyID = KeyID FROM Keys WHERE [Key] = @Key
	IF @@ROWCOUNT = 0
	BEGIN
		SET NOCOUNT ON
		INSERT INTO Keys ([Key]) VALUES (@Key)
		SET @KeyID = SCOPE_IDENTITY()
	END
	RETURN @KeyID
END
GO

/* Tests
SELECT TOP 100 * FROM v_Options ORDER BY 2, 3
DECLARE @KeyID int EXEC @KeyID = sp_AddKeyOption 'key1', 'value3'
*/
CREATE OR ALTER PROCEDURE sp_AddKeyOption @Key nvarchar(100), @Option nvarchar(100), @KeyID int = NULL OUTPUT AS
BEGIN -- Passing @KeyID is faster, but it's optional. If @KeyID is passed, value of @Key is ignored
	IF @KeyID IS NULL
		EXEC @KeyID = sp_GetKeyID @Key
	IF NOT EXISTS (SELECT KeyID FROM Options WHERE KeyID = @KeyID AND [Option] = @Option)
	BEGIN
		SET NOCOUNT ON
		INSERT INTO Options (KeyID, [Option]) VALUES (@KeyID, @Option)
	END
	RETURN @KeyID
END
GO

/* Tests
SELECT COUNT(*)/1000000 FROM Keys
SELECT TOP 100 * FROM Keys ORDER BY KeyID
DECLARE @KeyID int EXEC @KeyID = sp_GetRandomKeyID SELECT @KeyID AS KeyID
*/
CREATE OR ALTER PROCEDURE sp_GetRandomKeyID @KeyID int = NULL OUTPUT AS
BEGIN
	DECLARE @MinKeyID int
	DECLARE @MaxKeyID int
	DECLARE @RandomKeyID int
	SELECT @MinKeyID = Min(KeyID), @MaxKeyID = Max(KeyID) FROM Keys

	SET @RandomKeyID = FLOOR(RAND()*(@MaxKeyID+1-@MinKeyID))+@MinKeyID
	SELECT @KeyID = KeyID FROM Keys WHERE KeyID = @RandomKeyID
	IF @@ROWCOUNT = 0 -- Hit a hole, must have been deleted
	BEGIN
		SELECT @KeyID = Min(KeyID) FROM Keys WHERE KeyID > @RandomKeyID -- Get the next one in the sequence
		IF @@ROWCOUNT = 0
			SELECT @KeyID = Max(KeyID) FROM Keys WHERE KeyID < @RandomKeyID -- Try the last one in the sequence then
	END

	RETURN @KeyID
END
GO

/* Tests
SELECT COUNT(*)/1000000 FROM Keys
SELECT TOP 100 FROM Keys
DECLARE @KeyID int, @Key nvarchar(100) EXEC @KeyID = sp_GetRandomKey @Key = @Key OUTPUT SELECT @KeyID AS KeyID, @Key AS [Key]
*/
CREATE OR ALTER PROCEDURE sp_GetRandomKey @Key nvarchar(100) OUTPUT, @KeyID int = NULL OUTPUT AS
BEGIN
	EXEC @KeyID = sp_GetRandomKeyID
	SELECT @Key = [Key] FROM Keys WHERE KeyID = @KeyID
	RETURN @KeyID
END
GO

DROP PROCEDURE IF EXISTS sp_GetRandomKeyAndValue -- renamed to sp_GetRandomKeyAndOption
GO

/* Tests
SELECT TOP 100 * FROM v_Options ORDER BY [Key], [Option]
DECLARE @KeyID int, @Key nvarchar(100), @Option nvarchar(100) EXEC @KeyID = sp_GetRandomKeyAndValue @Key = @Key OUTPUT, @Option = @Option OUTPUT SELECT @KeyID AS KeyID, @Key AS [Key], @Option AS [Option]
*/
CREATE OR ALTER PROCEDURE sp_GetRandomKeyAndOption @Key nvarchar(100) OUTPUT, @Option nvarchar(100) OUTPUT, @KeyID int = NULL OUTPUT AS
BEGIN
	DECLARE @RandomKeyID int
	IF @KeyID IS NULL -- if passed a value, restricts the values to that particular KeyID
	BEGIN
		EXEC @RandomKeyID = sp_GetRandomKeyID
		SELECT TOP 1 @KeyID = @KeyID FROM Options WHERE KeyID = @RandomKeyID
	END
	IF @@ROWCOUNT = 0 -- Hit a hole, must have been deleted
	BEGIN
		SELECT @KeyID = Min(KeyID) FROM Options WHERE KeyID > @RandomKeyID -- Get the next one in the sequence
		IF @@ROWCOUNT = 0
			SELECT @KeyID = Max(KeyID) FROM Options WHERE KeyID < @RandomKeyID -- Try the last one in the sequence then
	END;

	WITH opts AS (SELECT ROW_NUMBER() OVER (ORDER BY [Option]) AS rowno, * FROM Options WHERE KeyID = @KeyID)
	SELECT @Key = k.[Key], @Option = opts.[Option]
	FROM opts
	JOIN Keys k ON (k.KeyID = opts.KeyID)
	WHERE opts.rowno = (SELECT FLOOR(RAND()*MAX(rowno))+1 FROM opts);

	RETURN @KeyID
END
GO

IF OBJECT_ID('tempdb..#NewOptions') IS NOT NULL DROP TABLE #NewOptions
CREATE TABLE #NewOptions ([Key] nvarchar(100) NOT NULL, [Option] nvarchar(100) NOT NULL)
GO

/* Tests
IF OBJECT_ID('tempdb..#NewOptions') IS NOT NULL DROP TABLE #NewOptions
CREATE TABLE #NewOptions ([Key] nvarchar(100) NOT NULL, [Option] nvarchar(100) NOT NULL)
INSERT INTO #NewOptions VALUES ('bulk1', 'bulkoption1')
INSERT INTO #NewOptions VALUES ('bulk1', 'bulkoption2')
INSERT INTO #NewOptions VALUES ('bulk1', 'bulkoption3')
INSERT INTO #NewOptions VALUES ('bulk1', 'bulkoption4')
INSERT INTO #NewOptions VALUES ('bulk2', 'bulkoption1')
INSERT INTO #NewOptions VALUES ('bulk2', 'bulkoption2')
INSERT INTO #NewOptions VALUES ('bulk2', 'bulkoption3')
INSERT INTO #NewOptions VALUES ('bulk2', 'bulkoption4')
SELECT * FROM #NewOptions
EXEC sp_AddKeysAndOptionsFromStaging
SELECT * FROM v_Options WHERE [Key] IN ('bulk1', 'bulk2')
DROP TABLE #NewOptions

SELECT COUNT(*)/(1000000) FROM Keys
SELECT COUNT(*)/(1000000) FROM Options
SELECT @@VERSION
*/
CREATE OR ALTER PROCEDURE sp_AddKeysAndOptionsFromStaging AS
BEGIN
	-- uses temp staging table #NewOptions, created as 
	-- Inserts all new keys and options from temporary table #NewOptions

	INSERT INTO Keys ([Key])
	SELECT DISTINCT [Key]
	FROM #NewOptions new
	WHERE NOT EXISTS (SELECT * FROM Keys WHERE [Key] = new.[Key])

	INSERT INTO Options (KeyID, [Option])
	SELECT DISTINCT k.KeyID, new.[Option]
	FROM #NewOptions new
	JOIN Keys k ON (k.[Key] = new.[Key])
	WHERE NOT EXISTS (SELECT KeyID FROM Options WHERE KeyID = k.KeyID AND [Option] = new.[Option])
END
GO

DROP TABLE #NewOptions
GO

