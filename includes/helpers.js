const getEventParam = (eventParamName, eventParamType = "string", columnName = false) => {
  let eventParamTypeName = "";
  switch (eventParamType) {
    case "string":
      eventParamTypeName = "string_value";
      break;
    case "int":
      eventParamTypeName = "int_value";
      break;
    case "double":
      eventParamTypeName = "double_value";
      break;
    case "float":
      eventParamTypeName = "float_value";
      break;
    default:
      throw "eventType is not valid";
  }
  return `(SELECT ep.value.${eventParamTypeName} AS ${eventParamName} FROM UNNEST(event_params) ep WHERE ep.key = '${eventParamName}') AS ${
    columnName ? columnName : eventParamName
  }`;
};

const getTableColumnsUnnestEventParameters = (eventParams)=>{
  return eventParams.map(eventParam => `${getEventParam(eventParam.name, eventParam.type)} `)
}

const getTableColumns= (params)=>{
  return params.map(param => `${param.name} as ${param.columnName}`)
}

const getDateFromTableName = (tblName) =>{
  return tblName.substring(7);
}
const getDatsetFromTableName = (tblName) =>{
  return tblName.substring(1,tblName.lastIndexOf('.'))

}

const getEventId = () => {
  return `FARM_FINGERPRINT(CONCAT(ifnull((SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'event_tag_timestamp'),event_timestamp), event_name, user_pseudo_id)) as event_id`
}

const getSessionId = () => {
return `FARM_FINGERPRINT(CONCAT((select value.int_value from unnest(event_params) where key = 'ga_session_id'), user_pseudo_id)) as session_id`
}

const getDate = () => `DATE(TIMESTAMP_MICROS(event_timestamp), "${constants.TIME_ZONE}") as date`;

const getDefaultChannelGroup = () => `CASE
      WHEN last_channel_info.source in ('direct','(direct)') and last_channel_info.medium in ("not set", "none", "(none)") THEN 'Direct'
      WHEN last_channel_info.medium in ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
      WHEN last_channel_info.source in ('email','e-mail','e_mail','e mail') or last_channel_info.medium in ('email','e-mail','e_mail','e mail') THEN 'Email'
      WHEN  last_channel_info.source in ('google','bing') and REGEXP_CONTAINS(last_channel_info.medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Search'
      WHEN  last_channel_info.source in ('linkedin', 'instagram', 'facebook', 't.co', 'tiktok', 'lnkd.in') and REGEXP_CONTAINS(last_channel_info.medium, r'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Social'
      WHEN last_channel_info.source in ('google','bing','yahoo','baidu','duckduckgo', 'yandex') or last_channel_info.medium = 'organic' THEN 'Organic Search'
      WHEN last_channel_info.source in ('linkedin', 'instagram', 'facebook', 't.co', 'tiktok', 'lnkd.in') or last_channel_info.medium in ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media') THEN 'Organic Social'
      WHEN last_channel_info.medium in ("referral", "app",  "link") THEN 'Referral'
      WHEN last_channel_info.source is null or last_channel_info.medium is null THEN ''
      ELSE 'Unassigned'
    END
  AS default_channel_group`


module.exports = {
  getEventParam,
  getDatsetFromTableName,
  getDateFromTableName,
  getTableColumns,
  getTableColumnsUnnestEventParameters,
  getEventId,
  getSessionId,
  getDate,
  getDefaultChannelGroup,
};