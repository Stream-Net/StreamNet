using System;
using System.Linq;
using System.Reflection;

namespace StreamNet.UnitTestingHelpers
{
    public static class UnitTestDetector
    {
        private static bool _isCalled = false;
        public static bool IsRunningFromUnitTesting()
        {
            if (_isCalled)
                return _isCalled;

            _isCalled = true;
            if (AppDomain.CurrentDomain.GetAssemblies().Any(x => x.FullName.ToLowerInvariant().Contains("test")))
            {
                return true;
            }
            
            if (AppDomain.CurrentDomain.GetAssemblies().Any(x => x.FullName.ToLowerInvariant().Contains("xunit")))
            {
                return true;
            }
            
            if (AppDomain.CurrentDomain.GetAssemblies().Any(x => x.FullName.ToLowerInvariant().Contains("nunit")))
            {
                return true;
            }
            
            if (AppDomain.CurrentDomain.GetAssemblies().Any(x => x.FullName.ToLowerInvariant().Contains("mstest")))
            {
                return true;
            }

            return false;
        }
    }
}