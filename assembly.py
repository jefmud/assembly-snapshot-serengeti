#!/home/mudayja/anderstm/shared/bin/python

# ### Description: Assemble one set of files-- empty, species, and exif-- into a single CSV file
import os
import sys
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='assembly.py - Assemble and merge SEASON RESULTS into CSV files')

parser.add_argument('--basedir', type=str, default='.', help='Base directory')
parser.add_argument('--resultsdir', type=str, default='results', help='Results directory default: ./results')
parser.add_argument('--season', type=str, required=True, help='Season name (required)')
parser.add_argument('--assemble', action='store_true', help='Assemble season')
parser.add_argument('--merge', action='store_true', help='Assemble merge')
parser.add_argument('--urlprefix', type=str, default='http://10.122.251.51:2019/', help='URL prefix of NAS location')


def assemble_one(dirname, index1, index2):
    """assemble one set of files-- empty, species, and exif-- into a single CSV file"""
    # Load the CSV files, this is fine as is.
    empty_df = pd.read_csv(f'{dirname}/assets_{index1}-{index2}_empty.csv', index_col='id')

    # get species data
    species_df = pd.read_csv(f'{dirname}/assets_{index1}-{index2}_species.csv', index_col='id')
    # we only want y_label == 'species' rows
    species_df = species_df[species_df['label'] == 'species']

    # get exif data
    exif_df = pd.read_csv(f'{dirname}/assets_{index1}-{index2}_exif.csv')
    # condition exif_df to include an id column, based on the filename minus the extension
    exif_df['id'] = exif_df['image'].str.split('.').str[0]

    # Merge the three dataframes using the common index
    merged_df = pd.merge(empty_df, species_df, on='id')
    merged_df = pd.merge(merged_df, exif_df, on='id')

    # Write the merged dataframe to a new CSV file
    merged_df.to_csv(f'{dirname}/assets_{index1}-{index2}_merged.csv')

def assemble_all(dirname):
    """assemble all the files in a directory into merged CSV files"""
    # get the directory listing
    listing = os.listdir(dirname)
    # sort the listing in place
    listing.sort()
    
    # filter the listing to only include the "assets_x-y_empty.csv" files
    for file in listing:
        # for each set of files, call the assemble_one function
        if file.endswith('_empty.csv'):
            # split the filename to get the index values
            index1, index2 = file.split('_')[1].split('-')
            species_file = f'{dirname}/assets_{index1}-{index2}_species.csv'
            exif_file = f'{dirname}/assets_{index1}-{index2}_exif.csv'
            
            # check for the existence of the species and exif files
            error = ""
            if not os.path.exists(species_file):
                error = f"Missing {species_file} "
            if not os.path.exists(exif_file):
                error += f"Missing {exif_file} "
            if error:
                print("CRITICAL: " + error)
                sys.exit(1)
                

            # call the assemble_one function
            print(f'Assemble {dirname}/assets_{index1}-{index2}_merged.csv')
            assemble_one(dirname, index1, index2)

def assemble_merged(season, dirname):
    # get the season
    
    # open the assets file to get the path to the file
    assets = pd.read_csv(f'./{season}_assets.csv')
    # rename the capture_id column to id
    assets.rename(columns={'capture_id': 'id'}, inplace=True)
    
    # get the directory listing
    listing = os.listdir(dirname)
    # sort the listing in place
    listing.sort()
    
    # filter the listing to only include the "assets_x-y_merged.csv" files
    master_df = pd.DataFrame()
    for file in listing:
        if file.endswith('_merged.csv'):
            # load the file
            print(f'Load {dirname}/{file}')
            df = pd.read_csv(f'{dirname}/{file}')
            # append the file to the master dataframe
            # use concat instead of append to avoid the warning
            #master_df = master_df.append(df)
            master_df = pd.concat([master_df, df])
    
    # merge the master dataframe with the assets dataframe
    master_df = pd.merge(master_df, assets, on='id')
    
    # create a URL column which is the prefix + the filename
    # default prefix of NAS appliance 'http://10.122.251.51:2019/'
    master_df = master_df.assign(url=args.urlprefix + master_df['pathname'])
    
    
    # write the master dataframe to a new CSV file
    master_df.to_csv(f'./{season}_master.csv')


# get the command line arguments
args = parser.parse_args()

# print the command line arguments
print(f'Base directory: {args.basedir}')
print(f'Results directory: {args.resultsdir}')
print(f'Season name: {args.season}')
print(f'Assemble season: {args.assemble}')
print(f'Assemble merge: {args.merge}')
print(f'URL prefix: {args.urlprefix}')
print()

if args.assemble:
    print("** Begin assemble **")
    dirname = f'{args.resultsdir}'
    assemble_all(dirname)
    print("** Assemble complete **")
    
if args.merge:
    print("** Begin merge **")
    dirname = f'{args.resultsdir}'
    assemble_merged(args.season, dirname)
    print("** Merge complete **")
