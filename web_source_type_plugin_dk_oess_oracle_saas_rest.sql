prompt --application/set_environment
set define off verify off feedback off
whenever sqlerror exit sql.sqlcode rollback
--------------------------------------------------------------------------------
--
-- ORACLE Application Express (APEX) export file
--
-- You should run the script connected to SQL*Plus as the Oracle user
-- APEX_200200 or as the owner (parsing schema) of the application.
--
-- NOTE: Calls to apex_application_install override the defaults below.
--
--------------------------------------------------------------------------------
begin
wwv_flow_api.import_begin (
 p_version_yyyy_mm_dd=>'2020.10.01'
,p_release=>'20.2.0.00.20'
,p_default_workspace_id=>2402141928452349
,p_default_application_id=>122
,p_default_id_offset=>29128759371994040
,p_default_owner=>'XXSUB'
);
end;
/
 
prompt APPLICATION 122 - ØSS Faktura Plus
--
-- Application Export:
--   Application:     122
--   Name:            ØSS Faktura Plus
--   Date and Time:   14:14 Wednesday January 13, 2021
--   Exported By:     MARNIL
--   Flashback:       0
--   Export Type:     Component Export
--   Manifest
--     PLUGIN: 9984375412850336
--   Manifest End
--   Version:         20.2.0.00.20
--   Instance ID:     204221214426117
--

begin
  -- replace components
  wwv_flow_api.g_mode := 'REPLACE';
end;
/
prompt --application/shared_components/plugins/web_source_type/dk_oess_oracle_saas_rest
begin
wwv_flow_api.create_plugin(
 p_id=>wwv_flow_api.id(9984375412850336)
,p_plugin_type=>'WEB SOURCE TYPE'
,p_name=>'DK.OESS.ORACLE_SAAS_REST'
,p_display_name=>unistr('\00D8SS Oracle SAAS REST')
,p_supported_ui_types=>'DESKTOP'
,p_api_version=>2
,p_wsm_capabilities_function=>'oss_saas_rest_plugin.capabilities_saas'
,p_wsm_fetch_function=>'oss_saas_rest_plugin.fetch_saas'
,p_wsm_discover_function=>'oss_saas_rest_plugin.discover_saas'
,p_substitute_attributes=>true
,p_subscribe_plugin_settings=>true
,p_help_text=>unistr('\00D8SS REST data Source type for SAAS. Supports pagination and server side filtering')
,p_version_identifier=>'1.0'
);
end;
/
prompt --application/end_environment
begin
wwv_flow_api.import_end(p_auto_install_sup_obj => nvl(wwv_flow_application_install.get_auto_install_sup_obj, false));
commit;
end;
/
set verify on feedback on define on
prompt  ...done
