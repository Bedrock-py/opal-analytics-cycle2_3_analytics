import csv
import pathlib
import traceback

from bedrock.analytics.api import results_collection
from bedrock.analytics.utils import Algorithm
import logging
import pandas as pd
from rpy2.robjects import r, pandas2ri
from rpy2.robjects.packages import importr
from rpy2.rinterface import RRuntimeError

import utils


class Analytics(Algorithm):

    def __init__(self):
        super(Analytics, self).__init__()
        self.parameters = []
        self.inputs = []
        self.outputs = ['matrix.csv', 'summary.txt']
        self.name = 'Cycle2 Analytics'
        self.type = 'GLM'
        self.description = 'Performs Stan GLM for NGS2 Cycle 2.'
        self.parameters_spec = [ ]

    def check_parameters(self):
        return True

    def __build_df__(self, filepath):
        pass
        # featuresPath = filepath['features.txt']['rootdir'] + 'features.txt'
        # matrixPath = filepath['matrix.csv']['rootdir'] + 'matrix.csv'
        # df = pd.read_csv(matrixPath, header=-1)
        # featuresList = pd.read_csv(featuresPath, header=-1)

        #df.columns = featuresList.T.values[0]

        # return df

    def custom(self, **kwargs):
        # df = self.__build_df__(filepath)

        from rpy2.robjects.packages import importr
        base = importr('base')
        # do things that generate R warnings
        base.warnings()

        pandas2ri.activate()

        r("setwd('{}')".format(kwargs["filepath"] + ""))
        r("load('.RData')")

        # save R workspace and load workspaces between opals!
        # 1. load workspace created during data load

        rstan = importr("rstanarm")
        # rdf = pandas2ri.py2ri(df)
        # robjects.globalenv["rdf"] = rdf

        opal_dir = pathlib.Path(__file__).parent

        # Note: BoomTown Metadata needs to be present too or it will fail without real error
        try:
            r("source('{}/analytics.R')".format(opal_dir))  # Load Wrangle
        except RRuntimeError as e:
            print(e)

        r("save.image()")  # saves to .RData by default

        # Get the GLM Results
        summary_text = r('summary(glmmoverall)')
        glm_result = r("data.frame(glmmoverall)")
        self.results = {'matrix.csv': list(csv.reader(glm_result.to_csv().split('\n'))), 'summary.txt': [str(summary_text)]}

        self.write_results(kwargs['filepath'] + "/output") # should it be results? + calling this since it's custom

        # store results in database

        try:
            # store metadata
            _, res_col = results_collection()
            src = res_col.find({'src_id': kwargs["src_id"]})[0]
            res_id = utils.getNewId()
            res = {}
            res['id'] = res_id
            res['rootdir'] = kwargs['filepath']
            res['name'] = kwargs["name"]
            res['src_id'] = kwargs["src_id"]
            res['created'] = utils.getCurrentTime()
            res['analytic_id'] = kwargs["analytic_id"]
            res['parameters'] = kwargs["parameters"]
            res['outputs'] = kwargs["outputs"]

            results = []
            for each in src['results']:
                results.append(each)
            results.append(res)
            res_col.update({
                'src_id': kwargs["src_id"]
            }, {'$set': {
                'results': results
            }})

            return res, 201

        except e:
            print(e)
            tb = traceback.format_exc()
            logging.error(tb)
            return tb, 406


        # rglmString = 'output <- stan_glmer({}, data = {}, family="{}")'.format(self.formula, "rdf", self.family)
        # logging.error(rglmString)
        # r(rglmString)
        # summary_txt = r('s<-summary(output)')
        # coef_table = r('data.frame(s$coefficients)')
        # self.results = {'matrix.csv': list(csv.reader(coef_table.to_csv().split('\n'))), 'summary.txt': [str(summary_txt)]}
