import matplotlib.pyplot as plt
import csv 

def performance_measurement_graph():
    # labels for bars
    labels = get_labels()
    
    mongodb_time = get_time('/Users/polinakuptsova/Documents/hslu/data-management/homework/mongodb/suppliers/execution_times_mongodb.txt')
    postgresql_time = get_time('/Users/polinakuptsova/Documents/hslu/data-management/homework/postgresql/suppliers/execution_times_postgresql.txt')

    x = range(len(labels))  # Generate x positions for the groups
    width = 0.35  # Width of each bar

    # Plotting both datasets side by side
    plt.bar([i - width/2 for i in x], mongodb_time, width, label='MongoDB', color='green')
    plt.bar([i + width/2 for i in x], postgresql_time, width, label='PostgreSQL', color='blue')

    # Adding labels and title
    plt.xlabel('Queries')
    plt.ylabel('Execution Time (ms)')
    plt.title('Query Performance Comparison')
    plt.xticks(x, labels)  # Set x-axis labels to query names
    plt.legend()

    plt.show()
    
# get the names of the queries 
def get_labels():
    filepath = '/Users/polinakuptsova/Documents/hslu/data-management/homework/postgresql/suppliers/execution_times_postgresql.txt' 
    labels = []
    with open(filepath, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            print(row[0])
            if row:  
                labels.append(row[0].strip()) 
    return labels
        
def get_time(file):
    times = []
    with open(file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in reader:
            if row:  
                times.append(float(row[1].strip()))

    return times; 

performance_measurement_graph()