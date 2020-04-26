SET SERVEROUTPUT ON;
BEGIN       
COMMON.LOG('Final logic');
END;
--------------------------------------------------------
---- #Create the tables# ----
-- DROP TABLE local_rm16;
CREATE TABLE local_rm16 AS SELECT * FROM v_nem_rm16 WHERE ROWNUM < 1;
--
-- DROP TABLE dbp_parameter;
CREATE table dbp_parameter AS SELECT * FROM DBP_ADMIN.dbp_parameter WHERE ROWNUM < 1;
--
-- DROP TABLE run_table;    
CREATE TABLE run_table(
    RUN_ID NUMBER, 
    RUN_START DATE,
    RUN_END DATE,
    OUTCOME VARCHAR2(15), --Assume that there are three values 'RUNNING', 'FINISHED' or NUll 
    REMARKS VARCHAR2(255)
);
--------------------------------------------------------
---- #Procedures and Functions# ----
CREATE OR REPLACE  FUNCTION fetch_Param(p_category U13305952.dbp_parameter.category%TYPE
                                     , p_code     U13305952.dbp_parameter.code%TYPE)
RETURN U13305952.dbp_parameter.value%TYPE
    IS
    v_value U13305952.dbp_parameter.value%TYPE;
BEGIN
    SELECT value INTO v_value 
        FROM U13305952.dbp_parameter
        WHERE category = p_category AND code = p_code;

    RETURN v_value;
END;
--
CREATE OR REPLACE FUNCTION is_Running
RETURN BOOLEAN 
    IS
    -- #Check if the there is an active instance --
    no_rows  run_table.run_id%TYPE;
    v_outcome run_table.outcome%TYPE;        
BEGIN
    SELECT count(*) INTO no_rows FROM run_table; 

    IF no_rows = 0
    THEN
        RETURN FALSE;
    ELSE
    SELECT outcome INTO v_outcome 
        FROM run_table
        WHERE ROWID = (SELECT MAX(ROWID) FROM run_table);
        RETURN v_outcome = 'RUNNING';
    END IF;
END;
--
CREATE OR REPLACE PROCEDURE run_New
    IS
    -- #Run if there is no active instance --
    running_interference EXCEPTION;
    v_max_id             run_table.run_id%TYPE;
    v_start_date         local_rm16.day%TYPE;
    v_end_date           local_rm16.day%TYPE;

BEGIN
    

    IF is_running()
    THEN
        RAISE running_interference;
    END IF;
    
    SELECT count(*) INTO v_max_id FROM run_table;
    v_start_date := TRUNC(sysdate);
    v_end_date := v_start_date + 13;
    
    INSERT INTO run_table VALUES (v_max_id + 1, v_start_date, v_end_date, 'RUNNING','');
    
    DELETE FROM local_rm16;  -- Delete current forecast data in local_rm16 

    EXCEPTION
        WHEN running_interference THEN
            dbms_output.put_line('There is an instance running');
END;
--
CREATE OR REPLACE PROCEDURE write_localrm16(p_tni local_rm16.tni%TYPE,
                                            p_frmp local_rm16.frmp%TYPE,
                                            p_lr local_rm16.lr%TYPE,
                                            p_hh local_rm16.hh%TYPE,
                                            p_day local_rm16.day%TYPE,
                                            p_volume local_rm16.volume%TYPE)
    IS
    -- #Write prediciton into local_rm16 --
BEGIN
    INSERT INTO local_rm16(tni, frmp, lr, hh, day, volume, statement_type, change_date) 
    VALUES (p_tni, p_frmp, p_lr, p_hh, p_day, p_volume, 'FORECAST', sysdate);
END;
--
CREATE OR REPLACE PROCEDURE calculate_mean(p_tni v_nem_rm16.tni%TYPE,
                                                   p_frmp v_nem_rm16.frmp%TYPE,
                                                   p_lr v_nem_rm16.lr%TYPE,
                                                   p_hh v_nem_rm16.hh%TYPE,
                                                   p_day v_nem_rm16.day%TYPE,
                                                   p_dotw varchar2)
    IS
    -- #Calculate the mean volume for a specific combination --
    v_mean v_nem_rm16.volume%TYPE;
BEGIN
    SELECT AVG(volume) INTO v_mean
    FROM v_nem_rm16
    WHERE 
        tni = p_tni
    AND frmp = p_frmp
    AND lr = p_lr
    AND hh = p_hh
    AND TO_CHAR(day, 'dy') = p_dotw
    AND day NOT IN (SELECT * FROM dbp_holiday);

    write_localrm16(p_tni, p_frmp, p_lr, p_hh, p_day, v_mean);
END;  
--
CREATE OR REPLACE PROCEDURE calculate_holiday_mean(p_tni v_nem_rm16.tni%TYPE,
                                                   p_frmp v_nem_rm16.frmp%TYPE,
                                                   p_lr v_nem_rm16.lr%TYPE,
                                                   p_hh v_nem_rm16.hh%TYPE,
                                                   p_day v_nem_rm16.day%TYPE)
    IS
    -- #Calculate the holiday mean volume for a specific combination --
    v_mean v_nem_rm16.volume%TYPE;
    v_count_holiday NUMBER;
BEGIN
    SELECT count(*) INTO v_count_holiday
    FROM v_nem_rm16
    WHERE day IN (SELECT * FROM dbp_holiday);
    
    IF (v_count_holiday > 0)
    THEN
        SELECT AVG(volume) INTO v_mean
        FROM v_nem_rm16
        WHERE 
            tni = p_tni
        AND frmp = p_frmp
        AND lr = p_lr
        AND hh = p_hh
        AND day IN (SELECT * FROM dbp_holiday);
    ELSE 
        SELECT AVG(volume) INTO v_mean
        FROM v_nem_rm16
        WHERE 
            tni = p_tni
        AND frmp = p_frmp
        AND lr = p_lr
        AND hh = p_hh
        AND TO_CHAR(day, 'dy') = 'SUN' ;
    END IF;
    
    write_localrm16(p_tni, p_frmp, p_lr, p_hh, p_day, v_mean);
END; 
--
CREATE OR REPLACE FUNCTION is_Holiday(p_date DATE)
RETURN BOOLEAN
    IS
    -- #Check if the input date is a holiday --
    v_count NUMBER;
BEGIN
    SELECT count(*) INTO v_count
    FROM dbp_holiday
    WHERE holiday_date = TRUNC(p_date);

    RETURN v_count = 1;
END;
--
CREATE OR REPLACE PROCEDURE predict
    IS
    -- #Predict for all the combinations in the next 14 days --
    v_current DATE := TRUNC(sysdate);
    v_counter NUMBER := 0;
    CURSOR c_combinations IS 
        SELECT DISTINCT tni, frmp, lr, hh FROM v_nem_rm16;
    v_mean v_nem_rm16.VOLUME % TYPE;

BEGIN
    WHILE v_counter < 14
    LOOP
        v_current := v_current + v_counter;
        FOR r_combi IN c_combinations
        LOOP
            IF is_Holiday(v_current)
            THEN
                calculate_holiday_mean(p_tni => r_combi.tni,  p_frmp => r_combi.frmp, p_lr => r_combi.lr, p_hh => r_combi.hh, p_day => v_current);
            ELSE
                calculate_mean(p_tni => r_combi.tni,  p_frmp => r_combi.frmp, p_lr => r_combi.lr, p_hh => r_combi.hh, p_day => v_current, p_dotw => TO_CHAR(v_current, 'dy'));
            END IF;           
        END LOOP;
        v_counter := v_counter + 1;
    END LOOP;
END;
--
CREATE OR REPLACE PROCEDURE finish_New
    IS
    -- #Update the outcome status of active instance to 'FINISHED'
BEGIN
    UPDATE run_table
    SET
        outcome = 'FINISHED'
    WHERE 
        ROWID = (SELECT MAX(ROWID) FROM run_table);
END;
--
CREATE OR REPLACE PROCEDURE main
    IS
    -- #Main --
BEGIN
    run_New();
    predict();
    finish_New();
END;