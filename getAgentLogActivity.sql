CREATE PROCEDURE [dbo].[getAgentLogActivity]	@p_agentname NVARCHAR(50),
												@p_agentloginid NVARCHAR(50), 
												@p_startTime DATETIME,
												@p_endTime DATETIME             
AS
BEGIN

	DECLARE @l_event_login			SMALLINT
	DECLARE @l_event_logout			SMALLINT
	DECLARE @l_eventtype			SMALLINT
	DECLARE @l_reasoncode			SMALLINT
	DECLARE @l_agentid 				INT
	DECLARE @l_eventdatetime 		DATETIME
	DECLARE @l_filter				BIT
	DECLARE @l_mineventdatetime 	DATETIME
	DECLARE @l_maxeventdatetime 	DATETIME
	DECLARE @SWV_cursor_var1 		CURSOR
	
	SET	@l_event_login = 1
	SET	@l_event_logout = 7
	
	DELETE FROM agentids
	
	INSERT INTO agentids (agentid, filter)
		SELECT
			  agentid
			 ,filter
		FROM selected_agents sa
		WHERE sa.agentname = @p_agentname
		AND sa.agentloginid = @p_agentloginid;
		
	DELETE FROM temp_asdr1
	
	INSERT INTO temp_asdr1(agentid, eventtype, eventdatetime, reasoncode)
		SELECT
			 asdr.agentid
			,asdr.eventtype
			,asdr.eventdatetime
			,asdr.reasoncode
		FROM 
			 agentstatedetail asdr
			,agentids ai 
		WHERE asdr.agentid = ai.agentid 
		AND asdr.eventdatetime BETWEEN @p_startTime AND  @p_endTime 
		AND asdr.eventtype IN(@l_event_login,@l_event_logout)
		
	UPDATE temp_asdr1
		SET filter = (SELECT
						filter
					  FROM agentids ai
		WHERE ai.agentid = temp_asdr1.agentid)
	
	DELETE FROM temp_asdr
	
	--alter sequence temp_asdr_seq 
	SELECT
		 @l_mineventdatetime = min(eventdatetime)
		,@l_maxeventdatetime = max(eventdatetime)
	FROM temp_asdr1
	WHERE filter = 1
	
	EXECUTE dbo.sp_executesql	 'l_mineventdatetime'
								,@l_mineventdatetime
	
	EXECUTE dbo.sp_executesql 	 'l_maxeventdatetime'
								,@l_maxeventdatetime
	
	IF @@rowcount = 0
	BEGIN
		SELECT
			 @l_mineventdatetime = NULL
			,@l_maxeventdatetime = NULL
		EXECUTE dbo.sp_executesql	'l_mineventdatetime'
									,@l_mineventdatetime
		EXECUTE dbo.sp_executesql	 'l_maxeventdatetime'
									,@l_maxeventdatetime
	END
	
	DELETE FROM temp_asdr1
	WHERE ((eventdatetime <= @l_mineventdatetime)
		OR (eventdatetime >= @l_maxeventdatetime))
		AND filter = 0
	
	SET @SWV_cursor_var1 = CURSOR FOR
	SELECT
		agentid,
		eventtype,
		eventdatetime,
		reasoncode,
		filter
	FROM temp_asdr1
	ORDER BY eventdatetime
	OPEN @SWV_cursor_var1
	FETCH NEXT FROM @SWV_cursor_var1 INTO @l_agentid, @l_eventtype, @l_eventdatetime, @l_reasoncode, @l_filter
	WHILE @@FETCH_STATUS = 0
	BEGIN
		-- WARNING: The INSERT statement was commented, because column name or number of supplied values does not match table definition.
		-- insert into temp_asdr(agentid, eventtype, eventdatetime, reasoncode, filter) values(temp_asdr_seq.nextval,l_agentid, l_eventtype, l_eventdatetime, l_reasoncode, l_filter);
		FETCH NEXT FROM @SWV_cursor_var1 INTO @l_agentid, @l_eventtype, @l_eventdatetime, @l_reasoncode, @l_filter
	END
	CLOSE @SWV_cursor_var1
	
	DELETE FROM temp_asdr1
	
	SELECT TOP 1
		@l_eventtype = eventtype,
		@l_eventdatetime = eventdatetime,
		@l_reasoncode = reasoncode,
		@l_filter = filter
	FROM temp_asdr
	WHERE eventdatetime = (SELECT
		MIN(eventdatetime)
	FROM temp_asdr)	
	
	IF @@rowcount = 0
		SELECT
			@l_eventtype = NULL,
			@l_eventdatetime = NULL,
			@l_reasoncode = NULL,
			@l_filter = NULL
	
	DELETE FROM temp_login_logout

	IF (@l_eventtype = @l_event_logout)
		INSERT INTO temp_login_logout (seq, logintime, op1, logouttime, reasoncode, loginfilter, logoutfilter)
			VALUES (0, NULL, '<', @l_eventdatetime, NULL, @l_filter, @l_filter)
		
	INSERT INTO temp_login_logout (seq, logintime, loginfilter)
		SELECT
			seq,
			eventdatetime,
			filter
		FROM temp_asdr tasdr
		WHERE eventtype = @l_event_login
	
	UPDATE temp_login_logout
	SET	logouttime = (SELECT
				eventdatetime
			FROM temp_asdr tasdr
			WHERE tasdr.eventtype = @l_event_logout
			AND tasdr.seq = (temp_login_logout.seq + 1)),
			logoutfilter = (SELECT
				filter
			FROM temp_asdr tasdr
			WHERE tasdr.eventtype = @l_event_logout
			AND tasdr.seq = (temp_login_logout.seq + 1)),
			reasoncode = (SELECT
				reasoncode
			FROM temp_asdr tasdr
			WHERE tasdr.eventtype = @l_event_logout
			AND tasdr.seq = (temp_login_logout.seq + 1))
			
	UPDATE temp_login_logout
	SET	logouttime = @p_endTime,
			op2 = '>',
			logoutfilter = 0
	WHERE logouttime IS NULL
	
	UPDATE temp_login_logout
	SET	logintime = NULL,
			op1 = '<',
			reasoncode = NULL
	WHERE loginfilter = 0
	AND logoutfilter = 1
	
	UPDATE temp_login_logout
	SET	logouttime = NULL,
			op2 = '>',
			reasoncode = NULL
	WHERE loginfilter = 1
	AND logoutfilter = 0
	
	DELETE FROM temp_login_logout
	WHERE loginfilter = 0
		AND logoutfilter = 0
	
	UPDATE temp_login_logout
	SET duration = dbo.datediff('ss', logintime, logouttime)
END
