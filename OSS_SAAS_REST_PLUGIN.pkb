create or replace PACKAGE BODY OSS_SAAS_REST_PLUGIN AS

  gc_scope_prefix constant varchar2(31) := lower($$PLSQL_UNIT) || '.';
  
  c_date_format CONSTANT VARCHAR2(20) := 'YYYY-MM-DD';
  c_limit       CONSTANT NUMBER := 500;
  
  --==============================================================================
  type t_summary is record(
      offset        number,
      count         number,
      hasMore       varchar2(10));
  
  --==============================================================================
  -- this function extracts result set information from the response JSON received
  -- from the "themoviedb.org" REST API. Each JSON response contains information
  -- about the total amount of pages, the page returned and the total amount of 
  -- results.
  --
  -- {"page":1,"total_results":84,"total_pages":5,"results":[{ ... }]}
  --==============================================================================
  function get_summary( p_response clob ) return t_summary 
  is
      l_summary t_summary;
  begin
      select offset,
             count,
             hasMore
        into l_summary.offset,
             l_summary.count,
             l_summary.hasMore
        from json_table( p_response,
                         '$'
                         columns ( 
                             offset     number path '$.offset',
                             count      number path '$.count',
                             hasMore    varchar2 path '$.hasMore' ) );
                             
      logger.log('Got summary: ' || l_summary.offset || '-' || l_summary.count || '-' || l_summary.hasMore);
      return l_summary;
      
  exception
    when no_data_found then
      return(l_summary);
  end get_summary;
  
  --==============================================================================
  -- REST Source Capabilities Procedure
  --
  -- This procedure tells APEX whether the Plug-In (and the REST API) supports
  -- pagination (large result sets come as multiple pages), server-side filtering
  -- and server-side ordering. 
  --
  -- The procedure implementation simply sets the "filtering", "pagination" or
  -- "order_by" members of the apex_plugin.t_web_source_capabilities record type
  -- to either true or false.
  --
  -- This plug-in supports the "themoviedb.org" pagination logic. Server Side 
  -- filtering or ordering are not supported.
  --==============================================================================
  procedure capabilities_saas (
      p_plugin         in            apex_plugin.t_plugin,
      p_result         in out nocopy apex_plugin.t_web_source_capabilities )
  is
  begin
      p_result.filtering            := true;
      p_result.pagination           := true;
      p_result.order_by             := true;
  end capabilities_saas;
  
  --==============================================================================
  -- REST Source Discovery Procedure
  --
  -- This procedure is called by APEX during the REST Data Source creation, when 
  -- the "Discover" button is clicked. This procedure can:
  -- * return structured information about the columns, data types and 
  --   JSON or XML selectors
  -- * return a JSON example which APEX then will sample to derive columns and
  --   data types
  --==============================================================================
  procedure discover_saas (
      p_plugin         in            wwv_flow_plugin_api.t_plugin,
      p_web_source     in            wwv_flow_plugin_api.t_web_source,
      p_params         in            wwv_flow_plugin_api.t_web_source_discover_params,
      p_result         in out nocopy wwv_flow_plugin_api.t_web_source_discover_result )
  is
      l_web_source_operation          apex_plugin.t_web_source_operation;
      l_dummy_parameters              apex_plugin.t_web_source_parameters;
      l_in_parameters                 apex_plugin.t_web_source_parameters;
      l_time_budget                   number;
  
      l_param_idx                     pls_integer;
      
      c_query_param_name    constant varchar2(5)    := 'q';
      c_query_param_limit   constant varchar2(10)   := 'limit';
      c_query_limit_value   constant varchar2(10)   := '50';
      l_query_param_value             varchar2(255) := NULL;
      l_has_query_param               boolean       := false;
  
      l_scope logger_logs.scope%type := gc_scope_prefix || 'discover_saas';
      l_params logger.tab_param;  
  BEGIN
     logger.log('START discover_saas');
    
      --
      -- discovery is based on the "fetch rows" operation of a REST Data Source; this is typically
      -- a GET operation. POST is also possible, but that must be configured in Shared Components
      -- REST Data Sources, Operations, Fetch Rows.
      --
      -- This gets all meta data on the REST Operation as an instance of APEX_PLUGIN.T_WEB_SOURCE_OPERATION.
      -- The P_PERFORM_INIT parameter determines whether APEX should compute the URL and initialize all 
      -- HTTP Headers and parameters with their default values. The "l_web_source_operation" represents
      -- all attributes of the HTTP operation to be made.
      -- 
      l_web_source_operation := apex_plugin_util.get_web_source_operation(
          p_web_source   => p_web_source,
          p_db_operation => apex_plugin.c_db_operation_fetch_rows,
          p_perform_init => true );
  
     logger.log('get_web_source_operation - done');
       
      --
      -- This section copies the parameters, which we receive from the Create REST data source
      -- wizard, to the "l_in_parameters" array. If a "query" parameter has been defined, we'll
      -- memorize the value and we'll use the default if no value was provided.
      -- 
      for i in 1..l_web_source_operation.parameters.count loop
          logger.log('found parameter:' || l_web_source_operation.parameters( i ).name || '=' || l_web_source_operation.parameters( i ).value);
          l_in_parameters( l_in_parameters.count + 1 ) := l_web_source_operation.parameters( i );
          if l_web_source_operation.parameters( i ).name = c_query_param_name then
              l_query_param_value := l_web_source_operation.parameters( i ).value;
              l_has_query_param   := true;
          end if;
      end loop;
  
      logger.log('parameters - done');
       
      --
      -- if the "query" parameter was provided by the developer, add it.
      --
      if not l_has_query_param then
          l_param_idx := l_in_parameters.count + 1;
          l_in_parameters( l_param_idx ).name       := c_query_param_name;
          l_in_parameters( l_param_idx ).param_type := wwv_flow_plugin_api.c_web_src_param_query;
      end if;
  
      --
      -- adjust the query string attribute of the REST operation to use the computed query parameter
      --
      l_web_source_operation.query_string := c_query_param_limit || '=' || c_query_limit_value;
  
      --
      -- perform the REST request. We'll receive the JSON response in the "p_result.sample_response" 
      -- variable.
      --
      logger.log('before make_rest_request, l_web_source_operation.query_string: ' || l_web_source_operation.query_string);
            
      apex_plugin_util.make_rest_request(
          p_web_source_operation => l_web_source_operation,
          p_bypass_cache         => false,
          --
          p_time_budget          => l_time_budget,
          --
          p_response             => p_result.sample_response,
          p_response_parameters  => l_dummy_parameters );
  
      -- set the response headers received by the REST API for display in the Discovery Results screen
      p_result.response_headers      := apex_web_service.g_headers;
      -- "api.themoviedb.org" uses a fixed page size of 20 results
      p_result.fixed_page_size       := 20;
      -- the "query" parameter can also be used for "row searches" (see REST Data Source Parameters)
      p_result.row_search_param_name := 'q';
      -- Computed Parameters to pass back to APEX
      p_result.parameters            := l_in_parameters;
      
  EXCEPTION
    WHEN OTHERS THEN
      logger.log_error('Unhandled Exception', l_scope, null, l_params);
      raise;    
  end discover_saas;
  
  --==============================================================================
  -- REST Source Fetch Procedure
  --
  -- This procedure does the actual "Fetch" operation when rows are being 
  -- requested from the REST Data Source. When an APEX component is about to
  -- render, APEX computes the first row and the amount of rows required. This
  -- and all dynamic filter and order by information is passed to the 
  -- procedure as the "p_params" parameter. 
  --==============================================================================
  procedure fetch_saas (
      p_plugin     in            apex_plugin.t_plugin,
      p_web_source in            apex_plugin.t_web_source,
      p_params     in            apex_plugin.t_web_source_fetch_params,
      p_result     in out nocopy apex_plugin.t_web_source_fetch_result )
  is
      l_web_source_operation apex_plugin.t_web_source_operation;
  
      l_time_budget          number;
      l_summary              t_summary;
      l_offset               pls_integer;
      l_first_offset         pls_integer;
      l_page_to_fetch        pls_integer := 0;
      l_continue_fetching    boolean     := true;
     
      c_page_size            pls_integer := coalesce( p_params.fixed_page_size, 20 );
  
      l_query_string         varchar2(32767);
      l_scope logger_logs.scope%type := gc_scope_prefix || 'discover_saas';
      l_params logger.tab_param;  
      l_query                apex_t_varchar2 := apex_t_varchar2();
      l_order_by             apex_t_varchar2 := apex_t_varchar2();
    
      -- Fetch the selector name for a column (column names are in upper case, and the 
      -- column selector is in the case which is understood by the service
      FUNCTION get_column_selector(p_name in APEX_APPL_DATA_PROFILE_COLS.name%TYPE) Return APEX_APPL_DATA_PROFILE_COLS.column_selector%TYPE IS
        l_column_selector APEX_APPL_DATA_PROFILE_COLS.column_selector%TYPE;
      BEGIN
        SELECT column_selector 
        INTO l_column_selector
        FROM APEX_APPL_DATA_PROFILE_COLS 
        WHERE data_profile_id = p_web_source.profile_id
        AND name = p_name;
        
        Return(l_column_selector);
      END;
      
       -- Fetch the data type for a column (column names are in upper case, and the 
      FUNCTION get_data_type(p_name in APEX_APPL_DATA_PROFILE_COLS.name%TYPE) Return APEX_APPL_DATA_PROFILE_COLS.data_type%TYPE IS
        l_data_type APEX_APPL_DATA_PROFILE_COLS.data_type%TYPE;
      BEGIN
        SELECT data_type 
        INTO l_data_type
        FROM APEX_APPL_DATA_PROFILE_COLS 
        WHERE data_profile_id = p_web_source.profile_id
        AND name = p_name;
        
        Return(l_data_type);
      END;
  begin
  
     logger.log('START fetch_saas');
     logger.log('fixed_page_size:' || p_params.fixed_page_size);
     logger.log('max_rows:' || p_params.max_rows);
     IF p_params.fetch_all_rows THEN
       logger.log('fetch_all_rows is TRUE');
     END IF;
  

      --
      -- This gets all meta data on the REST Operation as an instance of APEX_PLUGIN.T_WEB_SOURCE_OPERATION.
      -- The P_PERFORM_INIT parameter determines whether APEX should compute the URL and initialize all 
      -- HTTP Headers and parameters with their default values, from the REST Data Source configuration.
      -- The "l_web_source_operation" thus represents all attributes of the HTTP operation to be made.
      -- 
      l_web_source_operation := apex_plugin_util.get_web_source_operation(
          p_web_source   => p_web_source,
          p_db_operation => apex_plugin.c_db_operation_fetch_rows,
          p_perform_init => true );
  
      logger.log('after get_web_source_operation');

      -- Initialize the response output. An invocation of the "Fetch" procedure can also return multiple
      -- JSON or XML documents, so responses are maintained as an instance of the APEX_T_CLOB (array of CLOB) type
      p_result.responses := apex_t_clob();
                  
      --
      -- check whether the "query" parameter has a value. If not (empty query), we do not reach out to the
      -- REST API at all. For an empty query, api.themoviedb.org would return an error response; so it does
      -- not make any sense to perform the call. Instead, we simply return an empty JSON response ({}).
      --
      
      for i in 1 .. l_web_source_operation.parameters.count loop
          
          if l_web_source_operation.parameters( i ).name = 'q' and l_web_source_operation.parameters( i ).value is not null then
              apex_string.push(l_query, l_web_source_operation.parameters( i ).value);
          elsif l_web_source_operation.parameters( i ).value is not null then
              l_query_string := l_query_string || '&' || l_web_source_operation.parameters( i ).name  || '=' || l_web_source_operation.parameters( i ).value;
          end if;
      end loop;
                      
      -- Handle the external filters
      /* Filter types handled
      c_filter_eq              constant t_filter_type := 1;
      c_filter_not_eq          constant t_filter_type := 2;
      c_filter_gt              constant t_filter_type := 3;
      c_filter_gte             constant t_filter_type := 4;
      c_filter_lt              constant t_filter_type := 5;
      c_filter_lte             constant t_filter_type := 6;
      c_filter_null            constant t_filter_type := 7;
      c_filter_not_null        constant t_filter_type := 8;
      c_filter_contains        constant t_filter_type := 13;
      c_filter_not_contains    constant t_filter_type := 14;
     */
      FOR f IN 1..p_params.filters.count LOOP
        
        IF p_params.filters(f).column_name IS NULL THEN
          CONTINUE;
        END IF;
        
        -- Add query parameter
        APEX_STRING.PUSH(l_query, get_column_selector(p_params.filters(f).column_name) || ' ' ||
          CASE p_params.filters(f).filter_type
            WHEN apex_exec.c_filter_eq           THEN '='
            WHEN apex_exec.c_filter_not_eq       THEN '!='
            WHEN apex_exec.c_filter_gt           THEN '>'
            WHEN apex_exec.c_filter_gte          THEN '>='
            WHEN apex_exec.c_filter_lt           THEN '<'
            WHEN apex_exec.c_filter_lte          THEN '<='
            WHEN apex_exec.c_filter_null         THEN 'is null'
            WHEN apex_exec.c_filter_not_null     THEN 'is not null'
            WHEN apex_exec.c_filter_contains     THEN '='
            WHEN apex_exec.c_filter_not_contains THEN '=' 
            ELSE '='
          END || ' ' || 
          CASE
            WHEN p_params.filters(f).filter_values(1).date_value IS NOT NULL OR
                 get_data_type(p_params.filters(f).column_name) = 'DATE' THEN TO_CHAR(NVL(p_params.filters(f).filter_values(1).date_value, p_params.filters(f).filter_values(1).varchar2_value), c_date_format)
            WHEN p_params.filters(f).filter_values(1).number_value IS NOT NULL OR
                 get_data_type(p_params.filters(f).column_name) = 'NUMBER' THEN NVL(p_params.filters(f).filter_values(1).varchar2_value, TO_CHAR(p_params.filters(f).filter_values(1).number_value))
            WHEN p_params.filters(f).filter_values(1).varchar2_value IS NOT NULL THEN '''' || p_params.filters(f).filter_values(1).varchar2_value || ''''
          END);
        
      END LOOP;
      logger.log('2l_count COUNT:' || l_query.COUNT);
      
      -- Add the query string to the URL
      IF l_query.COUNT > 0 THEN
        logger.log('query parameters from APEX: ' || apex_string.join(l_query,';'));
        l_query_string := l_query_string || '&' || 'q=' || apex_string.join(l_query,';');
      END IF;
        
      l_offset := p_params.first_row - 1;
      l_first_offset := l_offset;
      logger.log('p_params.first_row: ' || p_params.first_row);
      
      -- Handle external order by
      FOR o IN 1..p_params.order_bys.COUNT LOOP
        APEX_STRING.PUSH(l_order_by, get_column_selector(p_params.order_bys(o).column_name) || ':' || 
          CASE p_params.order_bys(o).direction 
            WHEN apex_exec.c_order_asc THEN 'asc'
            ELSE 'desc'
          END);
        
        -- TODO: Currently only 1 order by is working. Should be fixed.
        EXIT;
      END LOOP;
    
      --
      -- if we are fetching all rows, fetch until the time budget is exhausted
      --
      while l_continue_fetching and coalesce( l_time_budget, 1 ) > 0 loop
  
          -- add a new member to the array of CLOB responses
          p_result.responses.extend( 1 );
          l_page_to_fetch := l_page_to_fetch + 1;
          
          --
          -- build the query string by using the operation attribute and appending the page to fetch
          -- query string example is: "query=star%20trek&page=2"
          --
          --l_web_source_operation.query_string := l_query_string || 'offset=' || l_page_id ;
          l_web_source_operation.query_string := 'offset=' || l_offset || '&limit=' || COALESCE(p_params.max_rows+1, c_limit) || l_query_string;
          IF l_order_by.COUNT > 0 THEN
             l_web_source_operation.query_string := l_web_source_operation.query_string || '&orderBy=' || APEX_STRING.JOIN(l_order_by,',');
          END IF;
          logger.log('query_string=' || l_web_source_operation.query_string);
          
          --
          -- perform the REST request. We'll receive the JSON response in the "p_result.sample_response" 
          -- variable. 
          --
          logger.log('make_rest_request');
          apex_plugin_util.make_rest_request(
              p_web_source_operation => l_web_source_operation,
              p_bypass_cache         => false,
              --
              p_time_budget          => l_time_budget,
              --
              p_response             => p_result.responses( l_page_to_fetch ),
              p_response_parameters  => p_result.out_parameters );
  
          -- Error handling
          IF apex_web_service.g_status_code NOT IN (200, 201) THEN
            logger.log_error('HTTP error status:' || apex_web_service.g_status_code);
          END IF;
          --
          -- call "get_summary" in order to retrieve the total amount of pages and the total amount
          -- of results, so that we know whether there are more pages ot not.
          --
          l_summary := get_summary( p_result.responses( l_page_to_fetch ) );
  
          l_offset := l_offset + l_summary.count;

          --
          -- if APEX requested "all rows" from the REST API and there are more rows to fetch,
          -- then continue fetching the next page 
          --
          l_continue_fetching := p_params.fetch_all_rows and l_summary.hasMore = 'true';
            
      end loop;
  
      --
      if p_params.fetch_all_rows then
          
          -- if APEX requested (and our logic fetched) all rows, then there are no more rows to fetch
          p_result.has_more_rows       := false;
          -- the JSON responses contains the total amount of rows
          p_result.response_row_count  := l_offset;
          -- the first row in the JSON responses is "1"
          p_result.response_first_row  := 1;
      else
          -- APEX did _not_ request all rows, so there might be another page. If the current page number is
          -- below the amount of total pages, then there are more rows to fetch
          p_result.has_more_rows       := (l_summary.hasMore = 'true');
          
          -- The JSON responses contain 20 rows (fixed page size) if there are more pages to fetch. If 
          -- we fetched the last page, we need to compute the amount of rows on that page.
          p_result.response_row_count  := l_offset;
  
          -- the first row in the JSON response depends on the page we started fetching with. 
          p_result.response_first_row  := l_first_offset+1;
          
          logger.log('result:' || l_summary.hasMore || '-' || p_result.response_row_count || '-' || p_result.response_first_row);
      end if;
      
      
    EXCEPTION
      WHEN OTHERS THEN
        logger.log_error('Unhandled Exception', l_scope, null, l_params);
        raise;    
  end fetch_saas;


END OSS_SAAS_REST_PLUGIN;