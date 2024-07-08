"""
@author: DKim

Customized single Sf-type reservoir to apply lag-function and convolution for runoff from impervious areas.
"""

try:
    from wflow.wf_DynamicFramework import *
except ImportError:
    from .wf_DynamicFramework import *

def impervious_no_lag(self):
    """
    This function is used when no unsaturated zone reservoir is used and only
    passes fluxes from the upper reservoirs to the lower
    Qf = Qf_in.
    Storage in fast reservoir = 0.
    """
    #self.Qfin_[k] = self.Qu * (1 - self.D)
    #self.Qf_[k] = self.Qfin_[k]
    #self.Sf_[k] = 0.0
    #if hasattr(self, 'wbSfimp_'):
    #    self.wbSfimp_ = (
    #        self.Qfimpin
    #        - self.Qfimp
    #        - self.Sfimp
    #        + self.Sfimp_t
    #        - sum(self.convQimp)
    #        + sum(self.convQimp_t)
    #    ) #WBtest Not sure yet

def impervious_lag(self):
    """
    - Lag is applied before inflow into the fast reservoir
    - Lag formula is derived from Fenicia (2011)
    - Outgoing fluxes are determined based on (value in previous timestep + inflow)
    and if this leads to negative storage, the outgoing fluxes are corrected to rato
    - not a semi analytical solution for Sf anymore
    - very fast responding reservoir to represent impervious runoff draining extra fast via roads and ditches
    """

    if self.FR_L:
        self.Qimpin = pcr.areatotal(
            (self.Pe) * self.percentArea, pcr.nominal(self.TopoId)
        )
    else:
        self.Qimpin = self.Pe

    #self.Qfimpin = self.Qimp

    # commented on 4 August 2015, as the output of this reservoir should not depent on the value of D
    #    if self.D[k] < 1.00:
    if self.convQimp:
        self.QfimpinLag = self.convQimp[-1]
        self.Qfimp = self.Sfimp * self.Kfimp
        sfimp_temp = self.Sfimp + self.QfimpinLag - self.Qfimp 
        self.Eimp = pcr.ifthenelse(sfimp_temp > self.PotEvaporation, self.PotEvaporation, 0)
        #self.Sfimp = pcr.ifthenelse((self.Sfimp + self.QfimpinLag - self.Qfimp - self.PotEvaporation)>0, (self.Sfimp + self.QfimpinLag - self.Qfimp - self.PotEvaporation), 0)
        #self.Sfimp = self.Sfimp + self.QfimpinLag - self.Qfimp
        self.Sfimp = sfimp_temp - self.Eimp

        self.convQimp.insert(
            0, 0 * pcr.scalar(self.catchArea)
        )  # convolution Qu for following time steps
        #self.Tfmap = self.Tfimp[0] * pcr.scalar(self.catchArea)
        del self.convQimp[-1]
        temp = [
            self.convQimp[i]
            + (2 / self.Tfimpmap - 2 / (self.Tfimpmap * (self.Tfimpmap + 1)) * (self.Tfimpmap - i))
            * self.Qimpin
            for i in range(len(self.convQimp))
        ]
        self.convQimp = temp

    else:
        self.Qfimp = self.Sfimp * self.Kfimp
        sfimp_temp = self.Sfimp + self.Qimpin - self.Qfimp 
        self.Eimp = pcr.ifthenelse(sfimp_temp > self.PotEvaporation, self.PotEvaporation, 0)
        self.Sfimp = sfimp_temp - self.Eimp

    # commented on 4 August 2015, as the output of this reservoir should not depent on the value of D
    #    else:
    #        self.Qfa = self.ZeroMap
    #        self.Qfain_[k] = self.ZeroMap
    if hasattr(self, 'wbSfimp_'):
        self.wbSfimp_ = (
            self.Qimpin
            - self.Qfimp
            - self.Sfimp
            + self.Sfimp_t
            - sum(self.convQimp)
            + sum(self.convQimp_t)
        ) #WBtest

    self.Qfimp_ = self.Qfimp