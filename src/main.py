import logging

__author__ = 'fmaia'

def main():

    #main loop on/off
    running = True
    #main loop number of runs
    runs = NUMBER_OF_RUNS
    ran = 0
    doStuff = False

    #RAMP UP
    if RAMPUP:
        time.sleep(240)
    #Main loop
    while(running):
        ran = ran + 1
        if VERBOSE:
            print 'ran: ',ran

        region_metrics = refreshStats()

        if ran == NUMBER_OF_SAMPLES:
            doStuff = True

        if (doStuff):
            if VERBOSE:
                print 'Process!'

            #process statistics
            process(region_metrics)
            #reset statistics
            stats = {}
            #mark as processed
            doStuff = False
            ran = 0
            print 'Finished processing.'

        time.sleep(LOOP_INTERVAL)
        runs = runs - 1
        if runs == 0:
            running = False

    print 'ENDED.'

if __name__ == '__main__':

    logging.basicConfig(filename='met.log', level=logging.INFO)
    logging.info('Started')


    print 'Starting.'

    main()
