"""
Worker for finding prime numbers using AFS
"""
import socket
import pickle
import time
import sys
import random
import asyncio
from primefinder2_snapshot.prime import is_prime
