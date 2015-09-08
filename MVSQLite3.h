//
//  MVSQLite3.h
//  SQLiteTools
//
//  Created by Michael Shabsin on 1/15/15.
//  Copyright (c) TurboFish Software All rights reserved.
//

#import <Foundation/Foundation.h>
#import <CoreData/CoreData.h>
#import <sqlite3.h>

typedef enum
{
    Success = 0,
    NoURLSpecified,
    NoPersistantStoreInMOC,
    DatabaseCouldNotBeOpened,
    CouldNotExecuteSelect,
    NoColumnsSpecifiedForUpdate,
    NumberOfColumnsAndValuesNotEqual,
    UpdateFailed
} MVsqlite3OperationReturnValues;

@interface MVSQLite3 : NSObject
{
    sqlite3* db;
    sqlite3_stmt* statement;
}

@property NSInteger currentStatus;

-(NSInteger)openDatabaseFromManagedObjectContext:(NSManagedObjectContext*)managedObjectContext;
-(NSInteger)openDatabaseAtURL:(NSURL*)dbURL;
-(NSInteger)closeDatabase;
-(NSArray*)selectFromTable:(NSString*)tableName;
-(NSArray*)selectColumns:(NSArray*)columnsToSelect fromTable:(NSString*)tableName;
-(id)selectMaxFromColumn:(NSString*)columnName forTable:(NSString*)tableName;
-(NSInteger)updateColumnsWithValues:(NSDictionary*)columnValueDictionary inTable:(NSString*)tableName where:(NSString*)whereClause;
-(NSInteger)updateColumns:(NSArray*)columns withValues:(NSArray*)values inTable:(NSString*)tableName where:(NSString*)whereClause;

@end
