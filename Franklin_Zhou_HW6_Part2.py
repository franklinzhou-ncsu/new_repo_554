# ST 554 Homework 6
# Franklin Zhou
# 3-9-2026

import matplotlib.pyplot as plt
import numpy as np
from numpy.random import default_rng
from sklearn import linear_model

class SLR_slope_simulator:

    # Initialize the class
    def __init__(self, beta_0, beta_1, x, sigma, seed):
        self.beta_0 = beta_0
        self.beta_1 = beta_1
        self.sigma = sigma
        self.x = x
        self.n = len(x)
        self.rng = default_rng(seed)
        self.slopes = []
    
    # generate_data method
    def generate_data(self):
        y = self.beta_0 + self.beta_1 * self.x + self.rng.normal(0, self.sigma, self.n) # generate y
        return y

    # fit_slope method
    def fit_slope(self, x,y):
        reg = linear_model.LinearRegression()
        reg.fit(self.x.reshape(-1, 1), y) # fit LM
        return reg.coef_[0] # return the slope
    
    # plot_sampling_distribution method
    def run_simulations(self, simulate_times):
        slopes = []
        for _ in range(simulate_times):
            y = self.generate_data() # call generate_data method to get x and y
            slope = self.fit_slope(self.x, y) # call fit_slope method to get slope
            slopes.append(slope) # save slopes
        self.slopes = np.array(slopes)

    # plot_sampling_distribution method
    def plot_sampling_distribution(self):
        if len(self.slopes) == 0:
            print("Please run the run_simulations() method first.")
            return
        else:
            plt.hist(self.slopes)
            plt.title("Sampling Distribution of Slope")
            plt.xlabel("Slope estimate")
            plt.ylabel("Frequency")
            plt.show()

    # find_prob method
    def find_prob(self, value, sided = "two-sided"):
        if len(self.slopes) == 0:
            print("Please run the run_simulations() method first.")
            return
        else:
            if sided == "two-sided": # update "two-sided" definition
                if value < np.median(self.slopes):
                    prob = 2 * np.mean(self.slopes < value)
                elif value > np.median(self.slopes):
                    prob = 2 * np.mean(self.slopes > value)
                else:
                    prob = 0.5
            elif sided == "above":
                prob = np.mean(self.slopes > value)
            elif sided == "below":
                prob = np.mean(self.slopes < value)
            return prob


# Creates an instance of the object 
ins = SLR_slope_simulator(
    beta_0 = 12,
    beta_1 = 2,
    x = np.array(list(np.linspace(start = 0, stop = 10, num = 11)) * 3),
    sigma = 1,
    seed = 10
)

# Error message
ins.plot_sampling_distribution()

# Run 10000 simulations
ins.run_simulations(10000)

# Plot the sampling distribution
ins.plot_sampling_distribution()

# Approximate the two-sided probability of being larger than 2.1
prob = ins.find_prob(2.1, sided = "two-sided")
print("The two-sided probability is:", prob)

# Print out the value of the simulated slopes using the attribute
print(ins.slopes)
