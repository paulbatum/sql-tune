var mssql = require('mssql'),
    M = require('mstring'),
    _ = require('underscore');

var indexQuery = M(function() {
    /***
    Select  *
    from
    (
        select
        (user_seeks+user_scans) * avg_total_user_cost * (avg_user_impact * 0.01) as index_advantage, migs.*
        from sys.dm_db_missing_index_group_stats migs
    ) as migs_adv,
    sys.dm_db_missing_index_groups mig,
    sys.dm_db_missing_index_details mid
    where
    migs_adv.group_handle = mig.index_group_handle and
    mig.index_handle = mid.index_handle
    and migs_adv.index_advantage > 10
    order by migs_adv.index_advantage DESC
    ***/ 
});

var fullQuery = M(function() {
    /***
    SELECT CONVERT (varchar, getdate(), 126) AS runtime, 
      mig.index_group_handle, mid.index_handle, 
      CONVERT (decimal (28,1), migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans)) AS improvement_measure, 
      'CREATE INDEX missing_index_' + CONVERT (varchar, mig.index_group_handle) + '_' + CONVERT (varchar, mid.index_handle) 
      + ' ON ' + mid.statement 
      + ' (' + ISNULL (mid.equality_columns,'') 
        + CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL THEN ',' ELSE '' END + ISNULL (mid.inequality_columns, '')
      + ')' 
      + ISNULL (' INCLUDE (' + mid.included_columns + ')', '') AS create_index_statement, 
      migs.*, mid.database_id, mid.[object_id]
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
    WHERE CONVERT (decimal (28,1), migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans)) > 10
    ORDER BY migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) DESC
    ***/
});

module.exports.init = function (cli) {

    var mobile = cli.category('mobile');
    var log = cli.output;

    var mobileSQL = mobile.category('sqldb');

    mobileSQL.description('Commands to manage SQL Database');

    mobileSQL.command('tune <servicename> <username> <password>')
        .description('Give the database a tuneup by creating missing indexes')
        .option('-i, --createIndexes', 'execute CREATE INDEX statements to create the missing indexes')
        .execute(function (servicename, username, password, options, callback) {

            var progress = cli.interaction.progress('Connecting to db');

            getSqlConfig(servicename, username, password, options, function(err, config) {
                if (err) {
                    progress.end();
                    callback(err);                    
                    return;
                }

                log.json('silly', config);

                var sqlConnection = new mssql.Connection(config, function(err) {
                    progress.end();
                    
                    if (err) {                        
                        callback(err);                    
                        return;
                    }

                    progress = cli.interaction.progress('Looking for missing index candidates');


                    var missingIndexRequest = new mssql.Request(sqlConnection);
                    missingIndexRequest.query(fullQuery, function(err, missingQueryResults) {
                        progress.end();
                        if (err) {                            
                            callback(err);                    
                            return;
                        }
                        log.json('silly', missingQueryResults);
                        
                        if(missingQueryResults.length == 0) {
                            log.info('No missing indexes found');
                            callback(null); 
                            return;   
                        }                        

                        var statements = missingQueryResults.map(function(row) {
                            return row.create_index_statement;
                            //return _.sprintf("CREATE INDEX missing_index_%s ON %s", row.index_group_handle)
                        });

                        log.info('The following indexes might improve the performance of your database:');
                        statements.forEach(function(stmt) {
                            log.info(stmt);
                        });
                        

                        if(options.createIndexes) {
                            progress = cli.interaction.progress('Executing CREATE INDEX statements');
                            var combined = statements.join('\n');
                            log.silly(combined);
                            var createIndexRequest = new mssql.Request(sqlConnection);
                            createIndexRequest.query(combined, function(err, results) {
                                progress.end();
                                if (err) {                            
                                    callback(err);                    
                                    return;
                                }
                                log.info("Indexes created.")
                                callback(null);                                
                            })
                            
                        } else {
                            log.info('The --createIndexes option was not specified - no changes will be made to the database'.yellow);
                            callback(null);
                        }
                        
                    });
                });
            });                
        });

    function getSqlConfig(servicename, username, password, options, callback) {
        options.servicename = servicename;
        options.json = true;

        mobile.getMobileServiceApplication(options, function (error, result) {
            if (error) {
                callback(error);
                return;
            }

            // Look up the server and database from the resource arrays
            var config = { options: { encrypt: true }},
                resources = _.union(result.InternalResources.InternalResource,
                                    result.ExternalResources.ExternalResource);

            resources.forEach(function (resource) {
                if (resource && resource.Type && resource.Name) {
                    if (resource.Type == 'Microsoft.WindowsAzure.SQLAzure.Server') {
                        config.server = resource.Name + '.database.windows.net';
                        config.user = username + '@' + resource.Name;
                        config.password = password;                                            
                    } else if (resource.Type == 'Microsoft.WindowsAzure.SQLAzure.DataBase') {
                        config.database = resource.Name;
                    }
                }
            });            

            callback(null, config);
        });
    }
};