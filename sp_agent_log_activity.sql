/*
Highlight and execute the following statement to drop the function
before executing the create statement.

DROP FUNCTION db_cra:sp_agent_log_activity;

*/

CREATE FUNCTION sp_agent_log_activity (@p_startTime DATETIME,
                                       @p_endTime DATETIME,
                                       @p_sortBy int = 0,
                                       @p_resGroupList VARCHAR(4000) = 'NULL',
                                       @p_agentList VARCHAR(4000) = 'NULL',
                                       @p_skillList VARCHAR(4000) = 'NULL',
                                       @p_teamList VARCHAR(4000) = 'NULL')
RETURNS @sp_agent_log_activity_tab TABLE ( Agent_Name NVARCHAR(50),
          Agent_Login_ID NVARCHAR(50),
          Agent_Extension NVARCHAR(50),
          op1 NVARCHAR(1),
          Login_Time DATETIME2(3),
          op2 NVARCHAR(1),
          Logout_Time DATETIME2(3),
          Logout_Reason_Code SMALLINT,
          Logon_Duration INT,
          latestSynchedTime DATETIME2(3))
BEGIN

    DECLARE @l_AgentName NVARCHAR(50), @l_AgentLoginID NVARCHAR(50);
    DECLARE @l_AgentExtension NVARCHAR(50);
    DECLARE @l_op1 NVARCHAR(1), @l_op2 NVARCHAR(1);
    DECLARE @l_LoginTime DATETIME2(3), @l_LogoutTime DATETIME2(3), @l_latestSynchedTime DATETIME2(3);
    DECLARE @l_LogoutReasonCode SMALLINT, @l_selType SMALLINT;
    DECLARE @l_LogonDuration INT, @l_resCount INT, @l_op INT;
    declare @l_selValue varchar(4000);
	

    DECLARE @l_eStartDate DATETIME2(3), @l_turncatedDate DATETIME2(3);
    DECLARE @l_IsDataScaledToSynch BIT, @l_isDataTruncated BIT;
 
    --BEGIN 
    --    ON EXCEPTION IN (-206,-8300) END EXCEPTION WITH RESUME;
    --        DROP TABLE selected_agents;
    --        DROP TABLE selected_names;
    --        DROP TABLE final_result;
    --        DROP TABLE temp_asdr;
	   -- DROP TABLE temp_asdr1;
    --        DROP TABLE temp_login_logout;
    --        DROP TABLE agentids;
    --        DROP SEQUENCE temp_asdr_seq;
            
    --END 

    --CREATE TEMP TABLE final_result (
    --    Agent_Name NVARCHAR(50),
    --    Agent_Login_ID NVARCHAR(50),
    --    Agent_Name2 NVARCHAR(50),
    --    agentloginid2 NVARCHAR(50),
    --    Agent_Extension NVARCHAR(50),
    --    op1 NVARCHAR(1),
    --    Login_Time DATETIME YEAR TO FRACTION(3),
    --    op2 NVARCHAR(1),
    --    Logout_Time DATETIME YEAR TO FRACTION(3),
    --    Logout_Reason_Code SMALLINT,
    --    Logon_Duration INT,
    --    oldname NVARCHAR(50), 
    --    rgselected1 smallint,
    --    skillSelected1 smallint,
    --    tmselected1 smallint,
    --    dateinactive1 datetime year to fraction(3),
    --    rgselected2 smallint,
    --    skillSelected2 smallint,
    --    tmselected2 smallint,
    --    dateinactive2 datetime year to fraction(3),
    --    latestSynchedTime DATETIME YEAR TO FRACTION(3)
    --) WITH NO LOG;
    
    -- contains the set of agents to be reported on
    DECLARE @selected_agents TABLE (
        agentloginid NVARCHAR(50), 
        agentname NVARCHAR(50),
        agentID INT,
        profileid INT,
        resourcegroupid int,
        dateinactive datetime,
        --filter boolean default 'f',
        rsmid INT,
        teamid INT
    );

	--CREATE TABLE #selected_agents (
 --       agentloginid NVARCHAR(50), 
 --       agentname NVARCHAR(50),
 --       agentID INT,
 --       profileid INT,
 --       resourcegroupid int,
 --       dateinactive datetime,	
 --       --filter boolean default 'f',
 --       rsmid INT,
 --       teamid INT
 --   )

    -- list of skills, agent names, or resource group names
    DECLARE @selected_names TABLE (name NVARCHAR(50));
	
	DECLARE @agentids TABLE (
		agentid int,
		filter bit default 'f'
	);

   --CREATE SEQUENCE temp_asdr_seq INCREMENT BY 1 START WITH 1;

    --chnaging seq from serial to int	
	DECLARE @temp_asdr TABLE (
		seq int,
		agentid int,
		eventtype smallint,
		reasoncode smallint,
		eventdatetime datetime,
		filter bit default 'f'
	);
	
	DECLARE @temp_asdr1 TABLE (
		agentid int,
		eventtype smallint,
		reasoncode smallint,
		eventdatetime datetime,
		filter bit default 'f'
	);
	
	DECLARE @temp_login_logout TABLE (
		seq int,
		logintime datetime,
		logouttime datetime,
		loginfilter bit default 'f',
		logoutfilter bit default 'f',
		reasoncode smallint,
		op1 nvarchar(1),
		op2 nvarchar(1),
		duration int
	);	
	
   SET @l_IsDataScaledToSynch = 'f';
   SET @l_eStartDate = @p_endTime;
   EXECUTE @p_endTime @l_eStartDate OUTPUT, @l_IsDataScaledToSynch OUTPUT;	

   IF @p_endTime > getutcdate() BEGIN
      SET @p_endTime = getutcdate();
   END
    
	IF @p_resGroupList <> 'NULL' BEGIN
       
           SET @l_selType = 1;
           SET @l_selvalue = @p_resGroupList;
	END
    -- list of agent login id's selected
    ELSE IF @p_agentList <> 'NULL' BEGIN

           SET @l_selType = 2;
           SET @l_selvalue = @p_agentList;
	END
    -- list of call skills selected
    ELSE IF @p_skillList <> 'NULL' BEGIN

           SET @l_selType = 3;
           SET @l_selvalue = @p_skillList;
	END	   
    -- list of teams selected
    ELSE IF @p_teamList <> 'NULL' BEGIN

           SET @l_selType = 4;
           SET @l_selvalue = @p_teamList;
	END	         
    -- default: all agents selected
    ELSE BEGIN

           SET @l_selType = 0;
           SET @l_selValue = null;
	END
    ---END;
	
   -- get list of agents to be reported
   EXEC dbo.sp_executesql @_p_startTime = @p_startTime, @_p_endTime = @p_endTime, @_l_selType = @l_selType, @_l_selValue = @l_selValue;
   
   DECLARE cur CURSOR FOR
   (SELECT [@agentname], [@agentloginid] 
		   from [@selected_agents])
   OPEN cur;
   FETCH cur INTO @l_AgentName, @l_AgentLoginID;
   WHILE @@FETCH_STATUS = 0 BEGIN
		   --getAgentLogActivity(@l_AgentName, @l_AgentLoginID, @p_startTime, @p_endTime);
		   insert into @sp_agent_log_activity_tab (Agent_Name, Agent_Login_ID, op1, Login_Time, op2, 
                           Logout_Time, Logout_Reason_Code, Logon_Duration)
			select @l_AgentName, @l_AgentLoginID, op1, logintime, op2, logouttime, reasoncode, duration
				from @temp_login_logout;
   FETCH cur INTO @l_AgentName, @l_AgentLoginID;
   end
   CLOSE cur;
   DEALLOCATE cur;
    
   -- CSCtz75943 
   EXEC dbo.sp_executesql @p_startTime, @p_endTime;   
						   
   SET @l_resCount = 0;

   SELECT @l_resCount = COUNT(*)
   FROM @sp_agent_log_activity_tab;
   
   IF @l_resCount <> 0 AND @l_IsDataScaledToSynch = 't' BEGIN
      
      INSERT INTO @sp_agent_log_activity_tab(Agent_Name, Agent_Login_ID, Logon_Duration, 
                                    latestSynchedTime)
           VALUES ('-------', '-------', -8888, @l_eStartDate);

	   update @sp_agent_log_activity_tab set latestSynchedTime = @l_eStartDate;
                  
   END 
 
  -- return the final result
   
    -- sort by login time
        IF @p_sortBy = 1 BEGIN

            DECLARE cur CURSOR FOR
				SELECT Agent_Name, Agent_Login_ID, Agent_Extension, op1, Login_Time, op2, 
                           Logout_Time, Logout_Reason_Code, Logon_Duration, latestSynchedTime
                                       FROM @sp_agent_log_activity_tab
                    ORDER BY Login_Time;
            OPEN cur;
            FETCH cur INTO @l_AgentName, @l_AgentLoginId, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime;
            WHILE @@FETCH_STATUS = 0 BEGIN

                    INSERT INTO @sp_agent_log_activity_tab VALUES (@l_AgentName, @l_AgentLoginID, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, 
                           @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime
                   );

            FETCH cur INTO @l_AgentName, @l_AgentLoginId, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime;
			--return
            END
            CLOSE cur;
            DEALLOCATE cur;
        END
        -- sort by logon duration
        ELSE IF @p_sortBy = 2 BEGIN

            DECLARE cur CURSOR FOR
				SELECT Agent_Name, Agent_Login_ID, Agent_Extension, op1, Login_Time, op2, 
                           Logout_Time, Logout_Reason_Code, Logon_Duration, latestSynchedTime
                FROM @sp_agent_log_activity_tab
                ORDER BY Logon_Duration;
            OPEN cur;
            FETCH cur INTO @l_AgentName, @l_AgentLoginId, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime;
            WHILE @@FETCH_STATUS = 0 BEGIN

                    INSERT INTO @sp_agent_log_activity_tab VALUES (@l_AgentName, @l_AgentLoginID, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, 
                           @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime
                   );

            FETCH cur INTO @l_AgentName, @l_AgentLoginId, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime;
            --return
			END
            CLOSE cur;
            DEALLOCATE cur;
        
        -- sort by agent name
        END
        ELSE BEGIN 

            DECLARE cur CURSOR FOR
					SELECT Agent_Name, Agent_Login_ID, Agent_Extension, op1, Login_Time, op2, 
                           Logout_Time, Logout_Reason_Code, Logon_Duration, latestSynchedTime
                    FROM @sp_agent_log_activity_tab
                    ORDER BY Agent_Name, Login_Time;
            OPEN cur;
            FETCH cur INTO @l_AgentName, @l_AgentLoginId, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime;
            WHILE @@FETCH_STATUS = 0 BEGIN

                    INSERT INTO @sp_agent_log_activity_tab VALUES (@l_AgentName, @l_AgentLoginID, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, 
                           @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime
                   );

            FETCH cur INTO @l_AgentName, @l_AgentLoginId, @l_AgentExtension, @l_op1, @l_LoginTime, @l_op2, @l_LogoutTime, @l_LogoutReasonCode, @l_LogonDuration, @l_latestSynchedTime;
            --return
			END
            CLOSE cur;
            DEALLOCATE cur;
                
        END 

		return

   --BEGIN 
   --     ON EXCEPTION IN (-206,-8300) END EXCEPTION WITH RESUME;
   --         DROP TABLE selected_agents;
   --         DROP TABLE selected_names;
   --         DROP TABLE final_result;
   --         DROP TABLE temp_asdr;
	  --  DROP TABLE temp_asdr1;
   --         DROP TABLE temp_login_logout;
   --         DROP TABLE agentids;
   --         DROP SEQUENCE temp_asdr_seq;
            
   -- END 
   
END;                                                                                                                                                                   


