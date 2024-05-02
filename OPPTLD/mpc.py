from mpi4py import MPI
from numba import njit

@njit(parallel=True)
def process_data(rank, size):
    tid = 0
    nthreads = 1
    msg = 0
    if rank == 0:
        nthreads = 1  # placeholder value for the number of threads in master rank
        print("%i threads in master rank" % nthreads)
        for i in range(1, size):
            comm.send(tid, dest=i, tag=tid)
    else:
        msg = comm.recv(source=0, tag=rank)
        print("Rank %i thread %i received %i" % (rank, tid, msg))

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    process_data(rank, size)

if __name__ == "__main__":
    main()
