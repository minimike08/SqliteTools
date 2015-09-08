//
//  MVDatabaseMaintenance.h
//  SQLiteTools
//
//  Created by Michael Shabsin on 1/15/15.
//  Copyright (c) TurboFish Software All rights reserved.
//

#import <Foundation/Foundation.h>
#import "MVSQLite3.h"

@interface MVDatabaseMaintenance : NSObject
{
    MVSQLite3* sqlite;
    NSURL* dbURL;
}

-(id)initWithManagedObjectContext:(NSManagedObjectContext*)managedObjectContext;
-(id)initWithURL:(NSURL*)dbURL;
-(NSInteger)prepareDatabaseForCoreData;

@end
