//
//  MVDatabaseMaintenance.m
//  SQLiteTools
//
//  Created by Michael Shabsin on 1/15/15.
//  Copyright (c) TurboFish Software All rights reserved.
//

#import "MVDatabaseMaintenance.h"

@implementation MVDatabaseMaintenance

-(id)initWithManagedObjectContext:(NSManagedObjectContext*)managedObjectContext
{
    if([[[managedObjectContext persistentStoreCoordinator] persistentStores] count] > 0)
        return [self initWithURL:[[[[managedObjectContext persistentStoreCoordinator] persistentStores] objectAtIndex:0] URL]];
    else
        return nil;
}

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

-(NSInteger)prepareDatabaseForCoreData
{
    if([sqlite openDatabaseAtURL:dbURL] != Success)
        return sqlite.currentStatus;
    
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
        
        NSString* whereClause = [NSString stringWithFormat:@"%@='%@'", Z_NAME_FIELD, [row objectForKey:Z_NAME_FIELD]];
        if([sqlite updateColumnsWithValues:@{@"Z_MAX":maxPK} inTable:@"Z_PRIMARYKEY" where:whereClause] != Success)
            NSLog(@"Unable to update Z_MAX for row %@ with value: %@", [row objectForKey:Z_NAME_FIELD], maxPK);
        
        if([sqlite updateColumnsWithValues:@{@"Z_OPT":@(1)} inTable:Z_TableName where:nil] != Success)
            NSLog(@"Unable to update Z_OPT for Table Name %@", Z_TableName);
        
    }
    
    return [sqlite closeDatabase];
}

@end
