import logging
import main_config
import time
import DecisionMaker
import Stats

__author__ = 'fmaia'

def main():

    #main loop on/off
    running = True
    #main loop number of runs
    runs = main_config.nloop
    ran = 0
    doStuff = False

    #RAMP UP
    if main_config.rampup:
        logging.info("Now sleeping for ramp up time of 240s.")
        time.sleep(240)

    logging.info('MeT in business.')
    stats = Stats.Stats()
    decision_maker = DecisionMaker.DecisionMaker(stats)

    #Main loop
    while running:
        ran = ran + 1

        logging.info('Running cycle %s' % str(ran))

        stats.refreshStats(True)

        if ran == main_config.nsamples:
            doStuff = True
        else:
            if not doStuff:
                time.sleep(main_config.sleeptime)

        if doStuff:
            logging.info('Going to process cluster status.')

            decision_maker.cycle(False)

            doStuff = False
            ran = 0
            stats.resetStats()
            runs = runs - 1
            logging.info('Finished round.')
            time.sleep(main_config.sleeptime)


        if runs == 0:
            running = False

    logging.info('MeT ended and EXITED.')

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(message)s',filename='met.log', level=logging.INFO)
    logging.info('Started MeT.')



    main()
