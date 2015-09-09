//
//  MVDatabaseMaintenance.m
//  SQLiteTools
//
//  Created by Michael Shabsin on 1/15/15.
//  Copyright (c) TurboFish Software All rights reserved.
//

#import "MVDatabaseMaintenance.h"

@implementation MVDatabaseMaintenance

// Initializes the tool using the database inside the ManagedObjectContext.
// Returns the initialized object.
-(id)initWithManagedObjectContext:(NSManagedObjectContext*)managedObjectContext
{
    if([[[managedObjectContext persistentStoreCoordinator] persistentStores] count] > 0)
        return [self initWithURL:[[[[managedObjectContext persistentStoreCoordinator] persistentStores] objectAtIndex:0] URL]];
    else
        return nil;
}

// Initializes the tool using the database located at the url.
// Returns the initialized object.
-(id)initWithURL:(NSURL*)url
{
    self = [super init];
    if(self)
    {
        sqlite = [MVSQLite3 new];
        dbURL = url;
    }
    return self;
}

// Updates all the tables in the database to set all the "Z_OPT" values to 1.
// Sets the "Z_MAX" values in the "Z_PRIMARYKEY" table to the max "Z_PK" value in each table.
-(NSInteger)prepareDatabaseForCoreData
{
    if([sqlite openDatabaseAtURL:dbURL] != Success)
        return sqlite.currentStatus;
    
    //Select all rows from CoreData's "Z_PRIMARYKEY" table.
    NSArray* primaryKeyTable = [sqlite selectFromTable:@"Z_PRIMARYKEY"];
    if(sqlite.currentStatus != Success)
        return sqlite.currentStatus;
    
    for(NSDictionary* row in primaryKeyTable)
    {
        
        NSString* Z_NAME_FIELD = @"Z_NAME";
        NSString* Z_TableName = [NSString stringWithFormat:@"Z%@",[row objectForKey:Z_NAME_FIELD]];
        NSNumber* maxPK = [sqlite selectMaxFromColumn:@"Z_PK" forTable:Z_TableName];
        if(!maxPK)
            continue;
        
        //Update the "Z_MAX" field for the current entry in the "Z_PRIMARYKEY" table.
        NSString* whereClause = [NSString stringWithFormat:@"%@='%@'", Z_NAME_FIELD, [row objectForKey:Z_NAME_FIELD]];
        if([sqlite updateColumnsWithValues:@{@"Z_MAX":maxPK} inTable:@"Z_PRIMARYKEY" where:whereClause] != Success)
            NSLog(@"Unable to update Z_MAX for row %@ with value: %@", [row objectForKey:Z_NAME_FIELD], maxPK);
        
        //Set the "Z_OPT" field to 1 for each entry in the "<Z_TableName>" table.
        if([sqlite updateColumnsWithValues:@{@"Z_OPT":@(1)} inTable:Z_TableName where:nil] != Success)
            NSLog(@"Unable to update Z_OPT for Table Name %@", Z_TableName);
        
    }
    
    return [sqlite closeDatabase];
}

@end
