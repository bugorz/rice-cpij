package edu.coursera.concurrent;

import edu.rice.pcdp.Actor;

import static edu.rice.pcdp.PCDP.finish;

/**
 * An actor-based implementation of the Sieve of Eratosthenes.
 *
 * Fill in the empty SieveActorActor actor class below and use it from
 * countPrimes to determine the number of primes <= limit.
 */
public final class SieveActor extends Sieve {

    /**
     * {@inheritDoc}
     *
     * Use the SieveActorActor class to calculate the number of primes <=
     * limit in parallel. You might consider how you can model the Sieve of
     * Eratosthenes as a pipeline of actors, each corresponding to a single
     * localPrime number.
     */
    @Override
    public int countPrimes(final int limit) {
        final SieveActorActor sieve = new SieveActorActor(2);
        finish(() -> {
            for (int i = 3; i <= limit; i += 2) {
                sieve.send(i);
            }
            sieve.send(0);
        });

        int numPrimes = 0;
        SieveActorActor actor = sieve;
        while (actor != null) {
            numPrimes += actor.getNumLocalPrimes();
            actor = actor.getNextActor();
        }

        return numPrimes;
    }

    /**
     * An actor class that helps implement the Sieve of Eratosthenes in
     * parallel.
     */
    public static final class SieveActorActor extends Actor {

        // Need to tune this variable
        private static final int MAX_LOCAL_PRIMES = 1000;
        private final int localPrimes[];
        private int numLocalPrimes;
        private SieveActorActor nextActor;

        public SieveActorActor(int localPrime) {
            this.localPrimes = new int[MAX_LOCAL_PRIMES];
            localPrimes[0] = localPrime;
            numLocalPrimes = 1;
            this.nextActor = null;
        }

        public int getNumLocalPrimes() {
            return numLocalPrimes;
        }

        public SieveActorActor getNextActor() {
            return nextActor;
        }

        /**
         * Process a single message sent to this actor.
         *
         * @param msg Received message
         */
        @Override
        public void process(final Object msg) {
            int candidate = (int) msg;

            if (candidate <= 0) {
                if (nextActor != null) {
                    nextActor.send(msg);
                }
                return;
            }

            final boolean isLocallyPrime = checkPrime(candidate);
            if (isLocallyPrime) {
                if (numLocalPrimes < MAX_LOCAL_PRIMES) {
                    localPrimes[numLocalPrimes] = candidate;
                    ++numLocalPrimes;
                } else if (nextActor == null) {
                    nextActor = new SieveActorActor(candidate);
                } else {
                    nextActor.send(msg);
                }
            }
        }

        private boolean checkPrime(final int candidate) {
            for (int i=0; i<numLocalPrimes; ++i) {
                int localPrime = localPrimes[i];
                if (candidate % localPrime == 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
