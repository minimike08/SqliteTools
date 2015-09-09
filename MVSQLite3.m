//
//  MVSQLite3.m
//  SQLiteTools
//
//  Created by Michael Shabsin on 1/15/15.
//  Copyright (c) TurboFish Software All rights reserved.
//

#import "MVSQLite3.h"

@implementation MVSQLite3

// Opens the database from the ManagedObjectContext.
// Returns 0 on success.
-(NSInteger)openDatabaseFromManagedObjectContext:(NSManagedObjectContext*)managedObjectContext
{
    if([[[managedObjectContext persistentStoreCoordinator] persistentStores] count] < 1)
        return NoPersistantStoreInMOC;
    
    NSURL* dbURL = [(NSPersistentStore*)[[[managedObjectContext persistentStoreCoordinator] persistentStores] objectAtIndex:0] URL];
    return [self openDatabaseAtURL:dbURL];
}

// Opens the database at the URL specified.
// Returns 0 on success.
-(NSInteger)openDatabaseAtURL:(NSURL*)dbURL
{
    if(!dbURL)
        return NoURLSpecified;
    self.currentStatus = sqlite3_open([[dbURL path] cStringUsingEncoding:NSUTF8StringEncoding], &db) == 0 ? Success :  DatabaseCouldNotBeOpened;
    return self.currentStatus;
}

// Closes database.
// Returns 0 on success.
-(NSInteger)closeDatabase
{
    return sqlite3_close(db);
}

// Convience method to select all columns in a table.
// Equivalent SQL statement: "SELECT * FROM <tableName>".
// Returns the selected rows as an array of dictionaries whose keys are the column names and whose values are the data contained in that row.
-(NSArray*)selectFromTable:(NSString*)tableName
{
    return [self selectColumns:nil fromTable:tableName];
}

// Selects the columns specified in the array from the specified table.
// Equivalent SQL statement: "SELECT <columnsToSelect[0]>, <columnsToSelect[1]>, ..., <columnToSelect[n-1]> FROM <tableName>".
// Returns the selected rows as an array of dictionaries whose keys are the column names and whose values are the data contained in that row.
-(NSArray*)selectColumns:(NSArray*)columnsToSelect fromTable:(NSString*)tableName
{
    if(!columnsToSelect || columnsToSelect.count == 0)
        columnsToSelect = @[@"*"];
    
    //Create string for SELECT statement.
    NSString* columnsString;
    for(NSString* column in columnsToSelect)
    {
        if(!columnsString)
            columnsString = column;
        else
            columnsString = [NSString stringWithFormat:@"%@, %@", columnsToSelect, column];
    }
    
    NSString* selectStatement = [NSString stringWithFormat:@"SELECT %@ FROM %@", columnsString, tableName];
    self.currentStatus = [self createSqlite3_stmtFromString:selectStatement];
    if(self.currentStatus != Success)
        return nil;
    
    //Enumerate all the returned rows and create an array of dictionaries of the key-values.
    NSMutableArray* result = [NSMutableArray new];
    NSArray* columnTypes;
    NSArray* columnNames = [self getColumnNamesForTable];
    int columns = sqlite3_column_count(statement);
    while(sqlite3_step(statement) == SQLITE_ROW)
    {
        if(!columnTypes)
            columnTypes = [self getColumnTypesForTable];
        
        NSMutableDictionary* row = [NSMutableDictionary new];
        for(int i = 0; i < columns; ++i)
        {
            switch ([columnTypes[i] intValue])
            {
                case SQLITE_INTEGER:
                     [row setObject:[NSNumber numberWithInt:sqlite3_column_int(statement, i)] forKey:columnNames[i]];
                    break;
                case SQLITE_FLOAT:
                     [row setObject:[NSNumber numberWithDouble:sqlite3_column_double(statement, i)] forKey:columnNames[i]];
                    break;
                case SQLITE_BLOB:
                     [row setObject:CFBridgingRelease(sqlite3_column_blob(statement, i)) forKey:columnNames[i]];
                    break;
                case SQLITE_TEXT:
                     [row setObject:[NSString stringWithUTF8String:(const char*)sqlite3_column_text(statement, i)] forKey:columnNames[i]];
                    break;
                default:
                    break;
            }
        }
        [result addObject:row];
    }
    sqlite3_finalize(statement);
    statement = 0;
    return result;
}

// Selects the max value in the column, columnName, from the table, tableName.
// Equivalent SQL statement: "SELECT MAX(<columnName>) FROM <tableName>".
// Returns the max value.
-(id)selectMaxFromColumn:(NSString*)columnName forTable:(NSString*)tableName
{
    NSString* selectStatement = [NSString stringWithFormat:@"SELECT MAX(%@) FROM %@", columnName, tableName];
    self.currentStatus = [self createSqlite3_stmtFromString:selectStatement];
    if(self.currentStatus != Success)
        return nil;
    
    
    //Execute the sql statement and return the value found.
    sqlite3_step(statement);
    int columnType = sqlite3_column_type(statement, 0);
    id result;
    switch (columnType)
    {
        case SQLITE_INTEGER:
            result = [NSNumber numberWithInt:sqlite3_column_int(statement, 0)];
            break;
        case SQLITE_FLOAT:
            result = [NSNumber numberWithDouble:sqlite3_column_double(statement, 0)];
            break;
        case SQLITE_BLOB:
            result = CFBridgingRelease(sqlite3_column_blob(statement, 0));
            break;
        case SQLITE_TEXT:
            result = [NSString stringWithUTF8String:(const char*)sqlite3_column_text(statement, 0)];
            break;
        default:
            result = nil;
            break;
    }
    sqlite3_finalize(statement);
    statement = 0;
    return result;
}

// Updates the columns in the columns array with the values array for the table, tableName, in rows matching the whereClause.
// Equivalent SQL statement: "UPDATE <tableName> SET <columns[0]>=<values[0]>, <columns[1]>=<values[1]>, ..., <columns[n-1]>=<values[n-1]> WHERE <whereClause>".
// Returns 0 on success.
-(NSInteger)updateColumns:(NSArray*)columns withValues:(NSArray*)values inTable:(NSString*)tableName where:(NSString*)whereClause
{
    if(columns.count != values.count)
        return NumberOfColumnsAndValuesNotEqual;
    
    NSMutableDictionary* updateDictionary = [NSMutableDictionary new];
    for(int i = 0; i < columns.count; ++i)
    {
        [updateDictionary setObject:values[i] forKey:columns[i]];
    }
    return [self updateColumnsWithValues:updateDictionary inTable:tableName where:whereClause];
}

// Updates the columns in the keys of the dictionary with their corresponding values for the table, tableName, in rows matching the whereClause.
// Equivalent SQL statement: "UPDATE <tableName> SET <key[0]>=<columnValueDictionary[ key[0] ]>, <key[1]>=<columnValueDictionary[ key[1] ]>, ..., <key[n-1]>=<columnValueDictionary[ key[n-1] ]> WHERE <whereClause>".
// Returns 0 on success.
-(NSInteger)updateColumnsWithValues:(NSDictionary*)columnValueDictionary inTable:(NSString*)tableName where:(NSString*)whereClause
{
    if(!columnValueDictionary || columnValueDictionary.count == 0)
        return NoColumnsSpecifiedForUpdate;
    
    //Create string for update statement.
    NSString* updateListString;
    for(NSString* column in columnValueDictionary)
    {
        if(!updateListString)
            updateListString = [NSString stringWithFormat:@"%@=%@", column, [columnValueDictionary objectForKey:column]];
        else
            updateListString = [NSString stringWithFormat:@"%@,%@=%@", updateListString, column, [columnValueDictionary objectForKey:column]];
    }
    
    whereClause = whereClause && ![whereClause isEqualToString:@""] ? [NSString stringWithFormat:@" WHERE %@", whereClause] : @"";
    NSString* updateStatement = [NSString stringWithFormat:@"UPDATE %@ SET %@%@", tableName, updateListString, whereClause];
    self.currentStatus = [self createSqlite3_stmtFromString:updateStatement];
    if(self.currentStatus != Success)
        return nil;
    
    int attempts, maxAttempts = 10;
    for(attempts = 0; attempts <= maxAttempts; ++attempts)
    {
        switch(sqlite3_step(statement))
        {
            case SQLITE_BUSY:
                continue;
            case SQLITE_DONE:
                self.currentStatus = Success;
                maxAttempts = -1;
                break;
            case SQLITE_ERROR:
                self.currentStatus = UpdateFailed;
                maxAttempts = -1;
                break;
        }
    }
    if(attempts == maxAttempts)
        self.currentStatus = UpdateFailed;
    sqlite3_finalize(statement);
    statement = 0;
    return self.currentStatus;
}

// Returns the data types of the columns in the table currently in use.
-(NSArray*)getColumnTypesForTable
{
    int columns = sqlite3_column_count(statement);
    NSMutableArray* columnTypes = [NSMutableArray new];
    for(int i = 0; i < columns; ++i)
    {
        [columnTypes addObject:[NSNumber numberWithInt:sqlite3_column_type(statement, i)]];
    }
    return columnTypes;
}

// Returns the names of the columns in the table currently in use.
-(NSArray*)getColumnNamesForTable
{
    int columns =  sqlite3_column_count(statement);
    NSMutableArray* columnNames = [NSMutableArray new];
    for(int i = 0; i < columns; ++i)
    {
        [columnNames addObject:[NSString stringWithUTF8String:sqlite3_column_name(statement, i)]];
    }
    return columnNames;
}

// Creates the sqlite statement required for operating on the database using a standard "SQL String".
// Returns 0 on success.
-(NSInteger)createSqlite3_stmtFromString:(NSString*)statementString
{
    const char* tail;
    return sqlite3_prepare_v2(db, [statementString cStringUsingEncoding:NSUTF8StringEncoding], [statementString lengthOfBytesUsingEncoding:NSUTF8StringEncoding], &statement, &tail);
}
@end
