package com.gravity.utilities

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
  * This is a set of functions and variables that are expected to be passed in from the outside.  This is to be separated from
  * the Settings and Settings2 scenarios which are wrapping the settings files themselves.  This helps isolate the initialization steps.
  *
  * Why is this private?  Because client code should be using the Settings.  This stuff is to help initialize the environment.  Settings can override these properties.
  */
private[utilities] object SystemPropertyHelpers {

  /**
    * If a system property is passed, will retrieve that.  Will fall back to an environment variable.  If neither, returns the provided default
    * @param propName
    * @param envName
    * @param default
    * @return
    */
  def propertyOrEnv(propName:String, envName:String, default:String) : String = {

    val sysProp = System.getProperty(propName)

    if(sysProp == null || sysProp.isEmpty) {
      val envVar = System.getenv(envName)

      if(envVar == null || envVar.isEmpty) {
        default
      } else {
        envVar
      }
    }else {
      sysProp
    }
  }

  def roleProperty = propertyOrEnv("com.gravity.settings.role", "COM_GRAVITY_SETTINGS_ROLE", "DEVELOPMENT")
  def environmentProperty = propertyOrEnv("com.gravity.settings.environment", "COM_GRAVITY_SETTINGS_ENVIRONMENT", "development")

  def isProductionProperty = environmentProperty == "production"


  def settingsFileName = if(environmentProperty == "production") "settings.production.properties" else "settings.development.properties"

  def isAwsProperty = propertyOrEnv("com.gravity.settings.aws", "COM_GRAVITY_SETTINGS_AWS", "false") == "true"

}
