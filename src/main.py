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
        time.sleep(240)

    stats = Stats.Stats()
    decision_maker = DecisionMaker.DecisionMaker(stats)

    #Main loop
    while running:
        ran = ran + 1

        logging.info('Running cycle %s' % str(ran))
        stats.refreshStats(True)

        if ran == main_config.nsamples:
            doStuff = True

        if doStuff:
            logging.info('Going to process cluster status.')

            decision_maker.cycle(False)

            doStuff = False
            ran = 0
            stats.resetStats()
            logging.info('Finished round.')

        time.sleep(main_config.sleeptime)
        runs = runs - 1

        if runs == 0:
            running = False

    logging.info('EXITED.')

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(message)s',filename='met.log', level=logging.INFO)
    logging.info('Started')

    logging.info('Starting MeT.')

    main()
