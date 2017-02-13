USE [master]
GO

IF OBJECT_ID('dbo.sp_foreachdb') IS NOT NULL
	DROP PROCEDURE dbo.sp_foreachdb
GO

CREATE PROCEDURE [dbo].[sp_foreachdb]
	--sp_MSForEachDB's parameters
	@command1 NVARCHAR(MAX) = NULL
	,@replacechar NCHAR(1) = N'?'
	,@command2 NVARCHAR(MAX) = NULL
	,@command3 NVARCHAR(MAX) = NULL
	,@precommand NVARCHAR(2000) = NULL
	,@postcommand NVARCHAR(2000) = NULL
	--Aaron Bertrand's parameters
	,@command NVARCHAR(MAX) = NULL --Treat as command0
	,@replace_character NCHAR(1) = N'?'
	,@print_dbname BIT = 0
	,@print_command_only BIT = 0
	,@suppress_quotename BIT = 1
	,@system_only BIT = NULL
	,@user_only BIT = NULL
	,@name_pattern NVARCHAR(300) = N'%'
	,@database_list NVARCHAR(MAX) = NULL
	,@exclude_list NVARCHAR(MAX) = NULL
	,@recovery_model_desc NVARCHAR(120) = NULL
	,@compatibility_level TINYINT = NULL
	,@state_desc NVARCHAR(120) = N'ONLINE'
	,@is_read_only BIT = 0
	,@is_auto_close_on BIT = NULL
	,@is_auto_shrink_on BIT = NULL
	,@is_broker_enabled BIT = NULL
	,@stop_on_error BIT = 0
AS
BEGIN
	SET NOCOUNT ON;
	--Drop in replacement that accepts both Aaron Bertrand's parameters from 
	--https://www.mssqltips.com/sqlservertip/2201/making-a-more-reliable-and-flexible-spmsforeachdb/
	--as well as sp_MSForEachDB's parameters.
	DECLARE	@sql NVARCHAR(MAX)
			,@sql1 NVARCHAR(MAX)
			,@sql2 NVARCHAR(MAX)
			,@sql3 NVARCHAR(MAX)
			,@dblist NVARCHAR(MAX)
			,@exlist NVARCHAR(MAX)
			,@db NVARCHAR(300)
			,@i INT
			,@retval INT;

	--Check to make sure both replacement character values haven't been set.
	IF @replacechar <> N'?' AND @replace_character <> N'?'
	BEGIN
		PRINT 'Please use either @replace_character OR @replacechar, not both.'
		RETURN -1
	END
	IF @command IS NULL AND @command1 IS NULL AND @command2 IS NULL AND @command3 IS NULL
	BEGIN
		PRINT 'Please enter 1 command to execute.'
		RETURN -1
	END

	--In case of sp_msforeachdb syntax, switch to Aaron's parameter.
	IF @replacechar <> N'?' AND @replace_character = N'?'
		SET @replace_character = @replacechar

	IF @database_list > N''
	BEGIN
		;
		WITH n (n)
		AS
		(
			SELECT
				ROW_NUMBER() OVER (ORDER BY s1.name)
				- 1
			FROM sys.objects AS s1
			CROSS JOIN sys.objects AS s2
		)
		SELECT
			@dblist = REPLACE(REPLACE(REPLACE(x, '</x><x>',
			','), '</x>', ''),
			'<x>', '')
		FROM (
			SELECT DISTINCT
				x = 'N'''
				+ LTRIM(RTRIM(SUBSTRING(@database_list,
				n,
				CHARINDEX(',',
				@database_list
				+ ',', n) - n)))
				+ ''''
			FROM n
			WHERE n <= LEN(@database_list)
				AND SUBSTRING(',' + @database_list, n,
				1) = ','
			FOR
			XML PATH ('')
		) AS y (x);
	END
	-- Added for @exclude_list
	IF @exclude_list > N''
	BEGIN
		;
		WITH n (n)
		AS
		(
			SELECT
				ROW_NUMBER() OVER (ORDER BY s1.name)
				- 1
			FROM sys.objects AS s1
			CROSS JOIN sys.objects AS s2
		)
		SELECT
			@exlist = REPLACE(REPLACE(REPLACE(x, '</x><x>',
			','), '</x>', ''),
			'<x>', '')
		FROM (
			SELECT DISTINCT
				x = 'N'''
				+ LTRIM(RTRIM(SUBSTRING(@exclude_list,
				n,
				CHARINDEX(',',
				@exclude_list
				+ ',', n) - n)))
				+ ''''
			FROM n
			WHERE n <= LEN(@exclude_list)
				AND SUBSTRING(',' + @exclude_list, n,
				1) = ','
			FOR
			XML PATH ('')
		) AS y (x);
	END

	CREATE TABLE #x (
		db NVARCHAR(300)
	);

	SET @sql = N'SELECT name FROM sys.databases WHERE 1=1'
	+
		CASE
			WHEN @system_only = 1
				THEN ' AND database_id IN (1,2,3,4)'
			ELSE ''
		END
	+
		CASE
			WHEN @user_only = 1
				THEN ' AND database_id NOT IN (1,2,3,4)'
			ELSE ''
		END
	-- To exclude databases from changes	
	+
		CASE
			WHEN @exlist IS NOT NULL
				THEN ' AND name NOT IN (' + @exlist + ')'
			ELSE ''
		END +
		CASE
			WHEN @name_pattern <> N'%'
				THEN ' AND name LIKE N''%' + REPLACE(@name_pattern,
					'''', '''''')
					+ '%'''
			ELSE ''
		END +
		CASE
			WHEN @dblist IS NOT NULL
				THEN ' AND name IN (' + @dblist + ')'
			ELSE ''
		END
	+
		CASE
			WHEN @recovery_model_desc IS NOT NULL
				THEN ' AND recovery_model_desc = N'''
					+ @recovery_model_desc + ''''
			ELSE ''
		END
	+
		CASE
			WHEN @compatibility_level IS NOT NULL
				THEN ' AND compatibility_level = '
					+ RTRIM(@compatibility_level)
			ELSE ''
		END
	+
		CASE
			WHEN @state_desc IS NOT NULL
				THEN ' AND state_desc = N''' + @state_desc + ''''
			ELSE ''
		END
	+
		CASE
			WHEN @is_read_only IS NOT NULL
				THEN ' AND is_read_only = ' + RTRIM(@is_read_only)
			ELSE ''
		END
	+
		CASE
			WHEN @is_auto_close_on IS NOT NULL
				THEN ' AND is_auto_close_on = ' + RTRIM(@is_auto_close_on)
			ELSE ''
		END
	+
		CASE
			WHEN @is_auto_shrink_on IS NOT NULL
				THEN ' AND is_auto_shrink_on = ' + RTRIM(@is_auto_shrink_on)
			ELSE ''
		END
	+
		CASE
			WHEN @is_broker_enabled IS NOT NULL
				THEN ' AND is_broker_enabled = ' + RTRIM(@is_broker_enabled)
			ELSE ''
		END;

	INSERT #x
	EXEC sp_executesql @sql;

	IF @precommand IS NOT NULL
	BEGIN
		IF @print_command_only = 1
			PRINT @precommand
		ELSE
			EXEC @retval = sys.sp_executesql @precommand
	END

	IF @retval <> 0 AND @stop_on_error = 1
		RETURN @retval

	DECLARE c CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY FOR
	SELECT
		CASE
			WHEN @suppress_quotename = 1
				THEN db
			ELSE QUOTENAME(db)
		END
	FROM #x
	ORDER BY db;

	OPEN c;

	FETCH NEXT FROM c INTO @db;

	WHILE @@fetch_status = 0
	BEGIN
		SET @sql = REPLACE(@command, @replace_character, @db);
		SET @sql1 = REPLACE(@command1, @replace_character, @db);
		SET @sql2 = REPLACE(@command2, @replace_character, @db);
		SET @sql3 = REPLACE(@command3, @replace_character, @db);

		IF @print_command_only = 1
		BEGIN
			PRINT '/* For ' + @db + ': */' + CHAR(13) + CHAR(10)
			PRINT CHAR(13) + CHAR(10) + @sql + CHAR(13) + CHAR(10)
			+ CHAR(13) + CHAR(10);
			
			PRINT CHAR(13) + CHAR(10) + @sql1 + CHAR(13) + CHAR(10)
			+ CHAR(13) + CHAR(10);

			PRINT CHAR(13) + CHAR(10) + @sql2 + CHAR(13) + CHAR(10)
			+ CHAR(13) + CHAR(10);

			PRINT CHAR(13) + CHAR(10) + @sql3 + CHAR(13) + CHAR(10)
			+ CHAR(13) + CHAR(10);
		END
		ELSE
		BEGIN
			IF @print_dbname = 1
			BEGIN
				PRINT '/* ' + @db + ' */';
			END

			EXEC @retval = sp_executesql @sql;
			IF @retval <> 0 AND @stop_on_error = 1
			BEGIN
				CLOSE c;
				DEALLOCATE c;
				PRINT N'Failed on db ' + @db + N' attempting @command ' + @sql
				RETURN @retval;
			END
			EXEC @retval = sp_executesql @sql1;
			IF @retval <> 0 AND @stop_on_error = 1
			BEGIN
				CLOSE c;
				DEALLOCATE c;
				PRINT N'Failed on db ' + @db + N' attempting @command1 ' + @sql1
				RETURN @retval;
			END
			EXEC @retval = sp_executesql @sql2;
			IF @retval <> 0 AND @stop_on_error = 1
			BEGIN
				CLOSE c;
				DEALLOCATE c;
				PRINT N'Failed on db ' + @db + N' attempting @command2 ' + @sql2
				RETURN @retval;
			END
			EXEC @retval = sp_executesql @sql3;
			IF @retval <> 0 AND @stop_on_error = 1
			BEGIN
				CLOSE c;
				DEALLOCATE c;
				PRINT N'Failed on db ' + @db + N' attempting @command3 ' + @sql3
				RETURN @retval;
			END
		END

		FETCH NEXT FROM c INTO @db;
	END

	CLOSE c;
	DEALLOCATE c;

	IF @postcommand IS NOT NULL
	BEGIN
		IF @print_command_only = 1
			PRINT @postcommand
		ELSE
			EXEC @retval = sys.sp_executesql @postcommand
	END
	IF @retval <> 0 AND @stop_on_error = 1
			BEGIN
				RETURN @retval;
			END
END
