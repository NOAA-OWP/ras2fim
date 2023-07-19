import sys
class ModelUnitError(Exception):
    '''
    -p flag denotes the projection of RAS models. The unit of this projection must be either in feet or meter. 
    If none of these units are found, this exception is raised.
    Also, the unit inferred from -p must be similar to the unit mentioned in the RAS model prj file.
    '''

class NoConflatedModelError(Exception):
    '''
    We might have multilple feature ids from HEC-RAS models but none have been conflated to NWM reaches
    in the given HUC8.
    Ras2fim needs at least one conflated model (one HEC-RAS model that its cross sections intersect with NWM reaches)
    to create sub-models for intersected feature ids. When there is none, this exception is raised.
    The file "***_stream_qc.csv" in folder 02 should contain at least one record. For step 5, the code reads this file
    to start making sub HEC-RAS models. So, the file  "***_stream_qc.csv" is the best file to check for this issue.

    '''


def check_conflated_models_number (conflated_number):
    try:
        if conflated_number  == 0:
            raise NoConflatedModelError ("No HEC-RAS model was conflated into National Water Model (NWM) reaches in "
                                         "the given HUC8. Please check your HEC RAS models and HUC8 number (-w flag) "
                                         "and make sure there is at least one HEC-RAS model that intersects with one "
                                         "of the NWM reaches in the given HUC8.")
        else:
            return True
    except NoConflatedModelError as e:
        print(e)
        sys.exit()

