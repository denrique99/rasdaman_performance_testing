import numpy as np

### Python-Script to compare 2 CSV-files to check if the Query-Output is the same ###

def compare_csv_data(data1, data2):

    # Convert data to numpy arrays and cleans data from bracktes
    arr1 = np.array([float(x.strip('{} \n}')) for x in data1.split(',')])
    arr2 = np.array([float(x.strip('{} \n}')) for x in data2.split(',')])
    
    # Check array lengths
    print(f"Array lengths: {len(arr1)} vs {len(arr2)}")
    if len(arr1) != len(arr2):
        print("CSVs ARE NOT EQUAL!!!: Arrays have different lengths!")
        return
        
    # Compare arrays
    differences = np.abs(arr1 - arr2)
    max_diff = np.max(differences)
    
    print(f"Maximum difference: {max_diff}")
    if max_diff > 0:
        diff_indices = np.where(differences > 0)[0]
        print(f"Number of differences: {len(diff_indices)}")
        print("\nFirst 5 differences:")
        for idx in diff_indices[:5]:
            print(f"Index {idx}: {arr1[idx]} vs {arr2[idx]} (diff: {differences[idx]})")

# Load and compare
with open('openeo_resultQ1_WSL.csv', 'r') as f1, open('wcs_resultQ1_WSL.csv', 'r') as f2:
    data1 = f1.read()
    data2 = f2.read()
    
compare_csv_data(data1, data2)