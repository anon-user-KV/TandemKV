#include <iostream>
#include <vector>
#include <numeric>
#include <cmath>

double mean(const std::vector<double>& data) {
    return std::accumulate(data.begin(), data.end(), 0.0) / data.size();
}

double covariance(const std::vector<double>& x, const std::vector<double>& y) {
    double mean_x = mean(x);
    double mean_y = mean(y);
    double cov = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        cov += (x[i] - mean_x) * (y[i] - mean_y);
    }
    return cov / x.size();
}

double variance(const std::vector<double>& x) {
    double mean_x = mean(x);
    double var = 0.0;
    for (size_t i = 0; i < x.size(); ++i) {
        var += (x[i] - mean_x) * (x[i] - mean_x);
    }
    return var / x.size();
}

double linear_regression(const std::vector<double>& x, const std::vector<double>& y, double next_x) {
    double b1 = covariance(x, y) / variance(x);
    double b0 = mean(y) - b1 * mean(x);
    return b0 + b1 * next_x;
}

std::vector<double> predict_future_values(const std::vector<double>& data, int window_size, int future_steps) {
    std::vector<double> future_predictions;

    std::vector<double> current_data = data;

    for (int step = 0; step < future_steps; ++step) {
        std::vector<double> x(window_size);
        std::vector<double> y(window_size);

        for (int j = 0; j < window_size; ++j) {
            x[j] = j;
            y[j] = current_data[current_data.size() - window_size + j];
        }

        double next_x = window_size;
        double predicted_value = linear_regression(x, y, next_x);

        future_predictions.push_back(predicted_value);

        current_data.push_back(predicted_value);
    }

    return future_predictions;
}

int main() {
    std::vector<double> data = {1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 18, 20, 21, 23, 27, 28, 30, 33, 35, 37, 100, 23, 24};

    int window_size = 5;
    int future_steps = 5;

    std::vector<double> future_predictions = predict_future_values(data, window_size, future_steps);

    std::cout << "Predicting next " << future_steps << " points: " << std::endl;
    for (size_t i = 0; i < future_predictions.size(); ++i) {
        std::cout << "  point " << i + data.size() << ": " << future_predictions[i] << std::endl;
    }

    return 0;
}
