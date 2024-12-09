spark = SparkSession.builder \
    .appName("TrafficSignsCategoryCount") \
    .getOrCreate()

# Input parameter: Sign Post type (passed as a parameter)
import sys

if len(sys.argv) < 2:
    print("Usage: spark-submit job.py <Sign Post type>")
    sys.exit(1)

sign_post_type = sys.argv[1]  # Example: "Punched Telespar"

# Define the schema of the dataset
schema = """
    X DOUBLE,
    Y DOUBLE,
    OBJECTID INT,
    Sign_Type STRING,
    Size_ STRING,
    Supplement STRING,
    Sign_Post STRING,
    Year_Insta STRING,
    Category STRING,
    Notes STRING,
    MUTCD STRING,
    Ownership STRING,
    FACILITYID INT,
    Schools STRING,
    Location_Adjusted STRING,
    Replacement_Zone STRING,
    Sign_Text STRING,
    Set_ID INT,
    FieldVerifiedDate STRING,
    GlobalID STRING
"""

# Path to the dataset (replace with actual path)
# Path to the dataset (replace with actual file path)
csv_file_path = "/home/anuragc3/cs425/g59/MP4/Traffic_Signs_10000.csv"  # Update this path

# Read the dataset as a batch DataFrame
traffic_signs_df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .csv(csv_file_path)

# Filter the DataFrame based on the specified Sign Post type
filtered_df = traffic_signs_df.filter(col("Sign_Post") == sign_post_type)

# Group by 'Category' and count occurrences
category_counts = filtered_df.groupBy("Category").count()

# Show the results
category_counts.show(truncate=False)

# Stop the Spark Session
spark.stop()