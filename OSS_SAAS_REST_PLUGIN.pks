create or replace PACKAGE OSS_SAAS_REST_PLUGIN AS 

  procedure capabilities_saas (
      p_plugin         in            apex_plugin.t_plugin,
      p_result         in out nocopy apex_plugin.t_web_source_capabilities );
      
  procedure discover_saas (
      p_plugin         in            wwv_flow_plugin_api.t_plugin,
      p_web_source     in            wwv_flow_plugin_api.t_web_source,
      p_params         in            wwv_flow_plugin_api.t_web_source_discover_params,
      p_result         in out nocopy wwv_flow_plugin_api.t_web_source_discover_result );
      
  procedure fetch_saas (
      p_plugin     in            apex_plugin.t_plugin,
      p_web_source in            apex_plugin.t_web_source,
      p_params     in            apex_plugin.t_web_source_fetch_params,
      p_result     in out nocopy apex_plugin.t_web_source_fetch_result );      

END OSS_SAAS_REST_PLUGIN;