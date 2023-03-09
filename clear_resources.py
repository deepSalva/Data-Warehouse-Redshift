"""
This script delete and clear all resources in the Redshift cluster.
Run it only when all your work is saved and finish
"""

import cluster_connection


#### CAREFUL!!
# -- If you run this script all resources from the cluster will be deleted!!!
def main():
    cluster_connection.cluster_disconnect()


if __name__ == "__main__":
    main()
