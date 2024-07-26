def sql_0(railway_name):
    return f"""WITH
	SVOD AS (
		SELECT 
			replace(station_from_name, '"', '') AS `Ст. Отправления`,
            replace(railway_from_name, '"', '') AS `Дор. Отправления`,    
			station_to_name  AS `Ст. Назначения`,
            railway_to_name AS `Дор. Назначения`,               
			ifNull(vehicle_number, '') AS `№ Вагона`,
			container_number AS `Контейнер`, 
			container_foot AS `Тип контейнера`,
			shipment_document__number AS `Отправка`,
			goods_etsng AS `Наименование груза`,
			operation_datetime AS `SVOD.Дата отправки`
		FROM 
			audit.sales__execution_orders
		WHERE 
			`Дор. Отправления` = replace('{railway_name}','"','')
	),
	RKS_RZD AS ( 
		SELECT
			document_reasons_number,
			anyIf(document_reasons_number, esu_id = '0.02.01.01') OVER (PARTITION BY container_number, `Date_E`) AS `document_reasons_number_0.02.01.01`,
			IF (document_reasons_number = '', `document_reasons_number_0.02.01.01`, document_reasons_number) AS document_reasons_number_processed,
			service_details_order_id AS order_id,
			upperUTF8(service_details_container_number) AS container_number,
			esu_id,
			min(date_end) AS `Date_E`,
			sum(amount_in_rub_without_vat) AS `amount_in_rub_without_vat_sum`,
			sum(amount_in_contract_currency_without_vat) AS `amount_in_contract_currency_without_vat_sum`		
		FROM 
			rks__directly AS RD
		WHERE
			container_number IN (SELECT `Контейнер` FROM SVOD) AND
			esu_id IN ('0.01.01.01', '0.01.01.02', '0.01.01.04', '2.01.01.01', '0.02.01.01') AND --SELECT * FROM dict_service WHERE id IN ('0.01.01.01', '0.01.01.02', '0.01.01.04', '2.01.01.01', '0.02.01.01')
			document_reasons_number <> ''
		GROUP BY 
			document_reasons_number,
			order_id,
			container_number,
			esu_id
		HAVING
			`amount_in_rub_without_vat_sum` <> 0 OR 
			`amount_in_contract_currency_without_vat_sum` <> 0 
	),
	RKS_RZD AS (
		SELECT
			document_reasons_number_processed,
			container_number,
			sumIf(amount_in_rub_without_vat_sum              , esu_id = '0.01.01.01') AS `amount_in_rub_without_vat_sum_0.01.01.01`,
			sumIf(amount_in_contract_currency_without_vat_sum, esu_id = '0.01.01.01') AS `amount_in_contract_currency_without_vat_sum_0.01.01.01`,
			sumIf(amount_in_rub_without_vat_sum              , esu_id = '0.01.01.02') AS `amount_in_rub_without_vat_sum_0.01.01.02`,
			sumIf(amount_in_contract_currency_without_vat_sum, esu_id = '0.01.01.02') AS `amount_in_contract_currency_without_vat_sum_0.01.01.02`,
			sumIf(amount_in_rub_without_vat_sum              , esu_id = '0.01.01.04') AS `amount_in_rub_without_vat_sum_0.01.01.04`,
			sumIf(amount_in_contract_currency_without_vat_sum, esu_id = '0.01.01.04') AS `amount_in_contract_currency_without_vat_sum_0.01.01.04`,
			sumIf(amount_in_rub_without_vat_sum              , esu_id = '2.01.01.01') AS `amount_in_rub_without_vat_sum_2.01.01.01`,
			sumIf(amount_in_contract_currency_without_vat_sum, esu_id = '2.01.01.01') AS `amount_in_contract_currency_without_vat_sum_2.01.01.01`			
		FROM
			RKS_RZD
			INNER JOIN (SELECT DISTINCT `Контейнер`, `Отправка` FROM SVOD) AS SVOD ON RKS_RZD.container_number = SVOD.`Контейнер` AND RKS_RZD.document_reasons_number_processed = SVOD.`Отправка`
		GROUP BY 
			document_reasons_number_processed,
			container_number
		HAVING 
			`amount_in_rub_without_vat_sum_0.01.01.01` <> 0 OR 
			`amount_in_contract_currency_without_vat_sum_0.01.01.01` <> 0 OR
			`amount_in_rub_without_vat_sum_0.01.01.02` <> 0 OR 
			`amount_in_contract_currency_without_vat_sum_0.01.01.02` <> 0 OR
			`amount_in_rub_without_vat_sum_0.01.01.04` <> 0  OR 
			`amount_in_contract_currency_without_vat_sum_0.01.01.04` <> 0 OR	
			`amount_in_rub_without_vat_sum_2.01.01.01` <> 0 OR 
			`amount_in_contract_currency_without_vat_sum_2.01.01.01` <> 0   
	),	
	RKS_BEFORE_AFTER AS (
		SELECT 
			service_details_order_id                     AS order_id,
			upperUTF8(service_details_container_number)  AS container_number,
			DC.name                                      AS client_name, 
			DL_FROM.name                                 AS points_from_catalog_name,	
			DL_TO.name                                   AS points_to_catalog_namer,
			DSt_FROM.station_name                   AS station_name_from,		
			DSt_TO.station_name                     AS station_name_to,		
			DSe.name                                     AS service_name,
			min(date_end)                                AS `Date_E`,
			sum(amount_in_rub_without_vat)               AS `amount_in_rub_without_vat_sum`,
			sum(amount_in_contract_currency_without_vat) AS `amount_in_contract_currency_without_vat_sum`		
		FROM 
			rks__directly               AS RD	
			LEFT JOIN dict_counterparty AS DC       ON RD.client_number_id = DC.id
			LEFT JOIN dict_service      AS DSe      ON RD.esu_id = DSe.id 
			LEFT JOIN dict_location     AS DL_FROM  ON RD.service_details_points_from_catalog_id = DL_FROM.id
			LEFT JOIN dict_location     AS DL_TO    ON RD.service_details_points_to_catalog_id = DL_TO.id
			LEFT JOIN dict_stations     AS DSt_FROM ON RD.service_details_points_from_station_id = DSt_FROM.station_code
			LEFT JOIN dict_stations     AS DSt_TO   ON RD.service_details_points_to_station_id = DSt_TO.station_code
		WHERE
			esu_id IN ('0.01.01.01', '0.01.01.02', '0.01.01.04', '2.01.01.01') AND 
			container_number IN (SELECT DISTINCT `Контейнер` FROM SVOD) 
		GROUP BY 
			order_id,
			container_number,	
			client_name,
			DL_FROM.name,
			DL_TO.name,
			service_name,
			station_name_from,	
			station_name_to		
		HAVING
			`amount_in_rub_without_vat_sum` <> 0 OR 
			`amount_in_contract_currency_without_vat_sum` <> 0			
),
	RKS_BEFORE_AFTER AS ( 
		SELECT
			`SVOD.Контейнер`,`SVOD.Дата отправки`,
		    --------------------------------------------------------------------------------------------------------------------------------------------------------------------
			'BEFORE-->',
			--------------------------------------------------------------------------------------------------------------------------------------------------------------------
			IF(`before_Date_E` = '1970-01-01 03:00:00', Null, date_diff( DAY,`SVOD.Дата отправки`,`before_Date_E`)) AS before_date_diff,
			argMaxIf(`order_id`                                   , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_order_id`,
			argMaxIf(`container_number`                           , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_container_number`,
			argMaxIf(`client_name`                                , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_client_name`,
			argMaxIf(`points_from_catalog_name`                   , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_points_from_catalog_name`,
			argMaxIf(`points_to_catalog_namer`                    , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_points_to_catalog_namer`,
			argMaxIf(`station_name_from`                          , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_station_name_from`,
			argMaxIf(`station_name_to`                            , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_station_name_to`,
			argMaxIf(`service_name`                               , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_service_name`,
			argMaxIf(`Date_E`                                     , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_Date_E`,
			argMaxIf(`amount_in_rub_without_vat_sum`              , `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_amount_in_rub_without_vat_sum`,
			argMaxIf(`amount_in_contract_currency_without_vat_sum`, `Date_E`, `Date_E` <= `SVOD.Дата отправки`) AS `before_amount_in_contract_currency_without_vat_sum`,
			--------------------------------------------------------------------------------------------------------------------------------------------------------------------
			'<--BEFORE AFTER-->',
			--------------------------------------------------------------------------------------------------------------------------------------------------------------------
			IF(`after_Date_E` = '1970-01-01 03:00:00', Null, date_diff( DAY,`SVOD.Дата отправки`,`after_Date_E`)) AS after_date_diff,
			argMinIf(`order_id`                                   , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_order_id`,
			argMinIf(`container_number`                           , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_container_number`,
			argMinIf(`client_name`                                , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_client_name`,
			argMinIf(`points_from_catalog_name`                   , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_points_from_catalog_name`,
			argMinIf(`points_to_catalog_namer`                    , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_points_to_catalog_namer`,
			argMinIf(`station_name_from`                          , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_station_name_from`,
			argMinIf(`station_name_to`                            , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_station_name_to`,
			argMinIf(`service_name`                               , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_service_name`,
			argMinIf(`Date_E`                                     , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_Date_E`,
			argMinIf(`amount_in_rub_without_vat_sum`              , `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_amount_in_rub_without_vat_sum`,
			argMinIf(`amount_in_contract_currency_without_vat_sum`, `Date_E`, `Date_E` >= `SVOD.Дата отправки`) AS `after_amount_in_contract_currency_without_vat_sum`,
			--------------------------------------------------------------------------------------------------------------------------------------------------------------------
			'<--AFTER'
			--------------------------------------------------------------------------------------------------------------------------------------------------------------------
		FROM
			RKS_BEFORE_AFTER
			INNER JOIN SVOD ON SVOD.`Контейнер` = container_number
		GROUP BY
			SVOD.`Контейнер`,
			`SVOD.Дата отправки`
),
OBRABOTKA AS (
	SELECT
		SVOD.*,
		RKS_RZD.*
	FROM 
		SVOD
		LEFT JOIN RKS_RZD ON SVOD.`Контейнер` = RKS_RZD.container_number AND SVOD.`Отправка` = RKS_RZD.document_reasons_number_processed
	)
SELECT
	OBRABOTKA.*,
	RKS_BEFORE_AFTER.*
FROM
	OBRABOTKA
	LEFT JOIN RKS_BEFORE_AFTER ON OBRABOTKA.`Контейнер` = RKS_BEFORE_AFTER.`SVOD.Контейнер` AND OBRABOTKA.`SVOD.Дата отправки`  = RKS_BEFORE_AFTER.`SVOD.Дата отправки`
"""

def sql_1 (railway_name):
    return f"""CREATE TABLE audit._sales__execution_orders_tmp
			ENGINE = MergeTree()
			ORDER BY `SVOD.Дата отправки`
			AS (
            	{sql_0(railway_name)})"""

def sql_2(railway_name):
	return f"""CREATE OR REPLACE TABLE audit._sales__execution_orders_tmp
	ENGINE = MergeTree()
	ORDER BY `SVOD.Дата отправки`
	AS (
		SELECT * FROM audit._sales__execution_orders_tmp
		UNION ALL 
			SELECT * FROM ( 
				{sql_0(railway_name)})
	)"""
