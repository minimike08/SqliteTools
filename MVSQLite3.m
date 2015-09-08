//
//  MVSQLite3.m
//  SQLiteTools
//
//  Created by Michael Shabsin on 1/15/15.
//  Copyright (c) TurboFish Software All rights reserved.
//

#import "MVSQLite3.h"

@implementation MVSQLite3

-(NSInteger)openDatabaseFromManagedObjectContext:(NSManagedObjectContext*)managedObjectContext
{
    if([[[managedObjectContext persistentStoreCoordinator] persistentStores] count] < 1)
        return NoPersistantStoreInMOC;
    
    NSURL* dbURL = [(NSPersistentStore*)[[[managedObjectContext persistentStoreCoordinator] persistentStores] objectAtIndex:0] URL];
    return [self openDatabaseAtURL:dbURL];
}

-(NSInteger)openDatabaseAtURL:(NSURL*)dbURL
{
    if(!dbURL)
        return NoURLSpecified;
    self.currentStatus = sqlite3_open([[dbURL path] cStringUsingEncoding:NSUTF8StringEncoding], &db) == 0 ? Success :  DatabaseCouldNotBeOpened;
    return self.currentStatus;
}

-(NSInteger)closeDatabase
{
    return sqlite3_close(db);
}

-(NSArray*)selectFromTable:(NSString*)tableName
{
    return [self selectColumns:nil fromTable:tableName];
}

-(NSArray*)selectColumns:(NSArray*)columnsToSelect fromTable:(NSString*)tableName
{
    if(!columnsToSelect || columnsToSelect.count == 0)
        columnsToSelect = @[@"*"];
    
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

-(id)selectMaxFromColumn:(NSString*)columnName forTable:(NSString*)tableName
{
    NSString* selectStatement = [NSString stringWithFormat:@"SELECT MAX(%@) FROM %@", columnName, tableName];
    self.currentStatus = [self createSqlite3_stmtFromString:selectStatement];
    if(self.currentStatus != Success)
        return nil;
    
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

-(NSInteger)updateColumnsWithValues:(NSDictionary*)columnValueDictionary inTable:(NSString*)tableName where:(NSString*)whereClause
{
    if(!columnValueDictionary || columnValueDictionary.count == 0)
        return NoColumnsSpecifiedForUpdate;
    
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

-(NSInteger)createSqlite3_stmtFromString:(NSString*)statementString
{
    const char* tail;
    return sqlite3_prepare_v2(db, [statementString cStringUsingEncoding:NSUTF8StringEncoding], [statementString lengthOfBytesUsingEncoding:NSUTF8StringEncoding], &statement, &tail);
}
@end
